import EventEmitter from 'events';
import axios from 'axios';
import retry from 'async-retry';
import WebSocket from 'ws';
import { Counter, Histogram, register } from 'prom-client';
import { v4 as uuidv4 } from 'uuid';
import { createHmac } from 'crypto';

const DEFAULT_CONFIG = {
  baseURL: process.env.AGENT_BASE_URL || 'http://localhost:8080',
  maxRetries: 5,
  retryMinTimeout: 1000,
  retryMaxTimeout: 30000,
  heartbeatInterval: 30000,
  connectionTimeout: 10000,
  authType: 'bearer',
  ssl: {
    cert: process.env.SSL_CERT_PATH,
    key: process.env.SSL_KEY_PATH,
    ca: process.env.SSL_CA_PATH
  },
  metrics: true
};

export class AgentClient extends EventEmitter {
  constructor(config = {}) {
    super();
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.instance = this._createHttpClient();
    this.ws = null;
    this.heartbeatTimer = null;
    this.pendingRequests = new Map();
    this._initializeMetrics();
  }

  _createHttpClient() {
    return axios.create({
      baseURL: this.config.baseURL,
      timeout: this.config.connectionTimeout,
      httpsAgent: this.config.ssl ? 
        new https.Agent(this.config.ssl) : undefined,
      headers: this._getAuthHeaders()
    });
  }

  _getAuthHeaders() {
    switch(this.config.authType) {
      case 'bearer':
        return { Authorization: `Bearer ${this.config.token}` };
      case 'hmac':
        return { 'X-Auth-Signature': this._generateHmac() };
      case 'api-key':
        return { 'X-API-Key': this.config.apiKey };
      default:
        return {};
    }
  }

  _generateHmac() {
    const timestamp = Date.now();
    const signature = createHmac('sha256', this.config.secretKey)
      .update(`${timestamp}${this.config.nonce}`)
      .digest('hex');
    return `${timestamp}:${signature}`;
  }

  _initializeMetrics() {
    if (!this.config.metrics) return;

    this.metrics = {
      requestCounter: new Counter({
        name: 'agent_client_requests_total',
        help: 'Total number of client requests',
        labelNames: ['method', 'status']
      }),
      latencyHistogram: new Histogram({
        name: 'agent_client_request_latency_seconds',
        help: 'Request latency distribution',
        buckets: [0.1, 0.5, 1, 2.5, 5, 10]
      }),
      errorCounter: new Counter({
        name: 'agent_client_errors_total',
        help: 'Total number of client errors',
        labelNames: ['type']
      })
    };
  }

  async initialize() {
    await this._setupWebSocket();
    this._startHeartbeat();
  }

  async _setupWebSocket() {
    if (this.ws) return;

    this.ws = new WebSocket(this.config.baseURL.replace('http', 'ws'), {
      handshakeTimeout: this.config.connectionTimeout,
      headers: this._getAuthHeaders()
    });

    this.ws.on('open', () => this.emit('connected'));
    this.ws.on('close', () => this.emit('disconnected'));
    this.ws.on('message', this._handleWsMessage.bind(this));
    this.ws.on('error', this._handleWsError.bind(this));
  }

  _handleWsMessage(data) {
    try {
      const message = JSON.parse(data);
      if (message.correlationId && this.pendingRequests.has(message.correlationId)) {
        const { resolve } = this.pendingRequests.get(message.correlationId);
        resolve(message);
        this.pendingRequests.delete(message.correlationId);
      } else {
        this.emit('event', message);
      }
    } catch (error) {
      this.emit('error', error);
    }
  }

  _handleWsError(error) {
    this.metrics?.errorCounter.inc({ type: 'websocket' });
    this.emit('error', error);
    this._scheduleReconnect();
  }

  _startHeartbeat() {
    this.heartbeatTimer = setInterval(() => {
      this.ws.ping();
    }, this.config.heartbeatInterval);
  }

  async _retryableRequest(fn, options = {}) {
    const start = Date.now();
    let lastError;

    try {
      return await retry(async (bail) => {
        try {
          const response = await fn();
          this._recordMetrics('success', start, options.method);
          return response;
        } catch (error) {
          lastError = error;
          if (this._shouldRetry(error)) {
            this._recordMetrics('retry', start, options.method);
            throw error;
          } else {
            bail(error);
          }
        }
      }, {
        retries: this.config.maxRetries,
        minTimeout: this.config.retryMinTimeout,
        maxTimeout: this.config.retryMaxTimeout
      });
    } catch (error) {
      this._recordMetrics('error', start, options.method);
      throw this._enhanceError(error, lastError);
    }
  }

  _shouldRetry(error) {
    return (
      !error.response ||
      error.response.status >= 500 ||
      error.code === 'ECONNABORTED' ||
      error.code === 'ETIMEDOUT'
    );
  }

  _recordMetrics(status, startTime, method) {
    if (!this.config.metrics) return;

    const latency = (Date.now() - startTime) / 1000;
    this.metrics.latencyHistogram.observe(latency);
    this.metrics.requestCounter.inc({ method, status });
  }

  _enhanceError(error, lastError) {
    error.isRetryable = this._shouldRetry(error);
    error.lastAttemptError = lastError;
    return error;
  }

  async sendCommand(command, payload) {
    const correlationId = uuidv4();
    return new Promise((resolve, reject) => {
      this.pendingRequests.set(correlationId, { resolve, reject });
      
      this.ws.send(JSON.stringify({
        type: 'command',
        command,
        payload,
        correlationId
      }), (error) => {
        if (error) reject(error);
      });
    });
  }

  async getAgentStatus(agentId) {
    return this._retryableRequest(
      () => this.instance.get(`/agents/${agentId}/status`),
      { method: 'GET' }
    );
  }

  async updateAgentConfiguration(agentId, config) {
    return this._retryableRequest(
      () => this.instance.post(
        `/agents/${agentId}/config`,
        config,
        { headers: { 'Content-Type': 'application/json' } }
      ),
      { method: 'POST' }
    );
  }

  streamLogs(agentId, callback) {
    const ws = new WebSocket(`${this.config.baseURL}/logs/${agentId}`);
    
    ws.on('message', (data) => {
      try {
        callback(JSON.parse(data));
      } catch (error) {
        this.emit('error', error);
      }
    });

    return () => ws.close();
  }

  async shutdown() {
    clearInterval(this.heartbeatTimer);
    if (this.ws) {
      this.ws.close();
    }
    this.removeAllListeners();
  }

  static exposeMetrics(port = 9090) {
    register.setDefaultLabels({ client: 'agent-client' });
    require('prom-client').collectDefaultMetrics();
    require('http').createServer(async (req, res) => {
      res.setHeader('Content-Type', register.contentType);
      res.end(await register.metrics());
    }).listen(port);
  }
}

// Usage example:
/*
const client = new AgentClient({
  authType: 'hmac',
  secretKey: process.env.SECRET_KEY,
  baseURL: 'https://agent-cluster.prod'
});

await client.initialize();

client.on('connected', () => console.log('Connected to agent cluster'));
client.on('event', (event) => handleEvent(event));

try {
  const status = await client.getAgentStatus('agent-123');
  await client.updateAgentConfiguration('agent-123', { logLevel: 'debug' });
  
  const stopLogStream = client.streamLogs('agent-123', (logEntry) => {
    console.log('Log:', logEntry);
  });
  
  // Later...
  stopLogStream();
} finally {
  await client.shutdown();
}
*/
