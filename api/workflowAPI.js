import express from 'express';
import helmet from 'helmet';
import cors from 'cors';
import { StatusCodes } from 'http-status-codes';
import { v4 as uuidv4 } from 'uuid';
import Joi from 'joi';
import knex from 'knex';
import redis from 'redis';
import { createLogger, format, transports } from 'winston';
import { Counter, Histogram, register } from 'prom-client';
import rateLimit from 'express-rate-limit';
import asyncHandler from 'express-async-handler';
import jwt from 'jsonwebtoken';

const app = express();
app.use(express.json());
app.use(helmet());
app.use(cors({ origin: process.env.CORS_ORIGIN || '*' }));

// ======================
// Configuration
// ======================
const config = {
  port: process.env.PORT || 3000,
  env: process.env.NODE_ENV || 'development',
  db: {
    client: 'pg',
    connection: process.env.DATABASE_URL || 'postgres://user:pass@localhost:5432/workflows'
  },
  redis: {
    url: process.env.REDIS_URL || 'redis://localhost:6379'
  },
  jwt: {
    secret: process.env.JWT_SECRET || 'super-secret-key',
    expiresIn: '15m'
  },
  rateLimit: {
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 100
  }
};

// ======================
// Database Setup
// ======================
const db = knex(config.db);
const redisClient = redis.createClient(config.redis);
redisClient.on('error', (err) => logger.error('Redis error:', err));

// ======================
// Logging
// ======================
const logger = createLogger({
  level: 'info',
  format: format.combine(
    format.timestamp(),
    format.json()
  ),
  transports: [
    new transports.Console(),
    new transports.File({ filename: 'workflow-api.log' })
  ]
});

// ======================
// Monitoring Metrics
// ======================
register.clear();
const metrics = {
  httpRequestCounter: new Counter({
    name: 'http_requests_total',
    help: 'Total HTTP requests',
    labelNames: ['method', 'route', 'status']
  }),
  httpRequestDuration: new Histogram({
    name: 'http_request_duration_seconds',
    help: 'HTTP request duration',
    labelNames: ['method', 'route'],
    buckets: [0.1, 0.5, 1, 2.5, 5, 10]
  }),
  workflowOperations: new Counter({
    name: 'workflow_operations_total',
    help: 'Workflow operation counters',
    labelNames: ['operation', 'status']
  })
};

// ======================
// Security Middleware
// ======================
const authenticate = (req, res, next) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) {
    return res.status(StatusCodes.UNAUTHORIZED).json({ error: 'Missing authorization header' });
  }

  const [bearer, token] = authHeader.split(' ');
  if (bearer !== 'Bearer' || !token) {
    return res.status(StatusCodes.UNAUTHORIZED).json({ error: 'Invalid token format' });
  }

  jwt.verify(token, config.jwt.secret, (err, decoded) => {
    if (err) {
      logger.warn('JWT verification failed:', err);
      return res.status(StatusCodes.UNAUTHORIZED).json({ error: 'Invalid token' });
    }
    req.user = decoded;
    next();
  });
};

// ======================
// Validation Schemas
// ======================
const workflowSchema = Joi.object({
  name: Joi.string().min(3).max(100).required(),
  description: Joi.string().max(500),
  version: Joi.number().min(1),
  steps: Joi.array().items(
    Joi.object({
      name: Joi.string().required(),
      action: Joi.string().required(),
      parameters: Joi.object(),
      retries: Joi.number().min(0).max(5),
      timeout: Joi.number().min(1)
    })
  ).min(1).required()
});

// ======================
// Rate Limiting
// ======================
const apiLimiter = rateLimit({
  windowMs: config.rateLimit.windowMs,
  max: config.rateLimit.max,
  handler: (req, res) => {
    res.status(StatusCodes.TOO_MANY_REQUESTS)
      .json({ error: 'Too many requests, please try again later' });
  }
});

// ======================
// Database Services
// ======================
class WorkflowService {
  async createWorkflow(workflowData) {
    const workflow = {
      id: uuidv4(),
      ...workflowData,
      created_at: db.fn.now(),
      updated_at: db.fn.now()
    };

    await db('workflows').insert(workflow);
    return workflow;
  }

  async getWorkflow(id) {
    return db('workflows').where({ id }).first();
  }

  async listWorkflows(pagination = { page: 1, pageSize: 20 }) {
    return db('workflows')
      .orderBy('created_at', 'desc')
      .paginate({ ...pagination, isLengthAware: true });
  }

  async executeWorkflow(workflowId, input) {
    const workflow = await this.getWorkflow(workflowId);
    if (!workflow) {
      throw new Error('Workflow not found');
    }

    const executionId = uuidv4();
    await redisClient.set(
      `execution:${executionId}`,
      JSON.stringify({ status: 'pending', steps: [] })
    );

    return { executionId, workflow };
  }
}

// ======================
// API Controllers
// ======================
class WorkflowController {
  constructor(service) {
    this.service = service;
  }

  createWorkflow = asyncHandler(async (req, res) => {
    const { error, value } = workflowSchema.validate(req.body);
    if (error) {
      return res.status(StatusCodes.BAD_REQUEST).json({ error: error.details });
    }

    const workflow = await this.service.createWorkflow(value);
    metrics.workflowOperations.inc({ operation: 'create', status: 'success' });
    res.status(StatusCodes.CREATED).json(workflow);
  });

  getWorkflow = asyncHandler(async (req, res) => {
    const workflow = await this.service.getWorkflow(req.params.id);
    if (!workflow) {
      return res.status(StatusCodes.NOT_FOUND).json({ error: 'Workflow not found' });
    }
    res.json(workflow);
  });

  executeWorkflow = asyncHandler(async (req, res) => {
    const { id } = req.params;
    const result = await this.service.executeWorkflow(id, req.body);
    metrics.workflowOperations.inc({ operation: 'execute', status: 'started' });
    res.status(StatusCodes.ACCEPTED).json(result);
  });
}

// ======================
// Middleware Pipeline
// ======================
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    const duration = Date.now() - start;
    metrics.httpRequestDuration
      .labels(req.method, req.route?.path || 'unknown')
      .observe(duration / 1000);
    metrics.httpRequestCounter
      .labels(req.method, req.route?.path || 'unknown', res.statusCode)
      .inc();
  });
  next();
});

app.use('/api', apiLimiter);
app.use(authenticate);

// ======================
// Route Definitions
// ======================
const workflowService = new WorkflowService();
const workflowController = new WorkflowController(workflowService);

app.post('/api/workflows', workflowController.createWorkflow);
app.get('/api/workflows/:id', workflowController.getWorkflow);
app.post('/api/workflows/:id/execute', workflowController.executeWorkflow);

// ======================
// Health & Metrics
// ======================
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.get('/metrics', async (req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

// ======================
// Error Handling
// ======================
app.use((err, req, res, next) => {
  logger.error(`Unhandled error: ${err.stack}`);
  metrics.httpRequestCounter
    .labels(req.method, req.route?.path || 'unknown', StatusCodes.INTERNAL_SERVER_ERROR)
    .inc();
  
  res.status(StatusCodes.INTERNAL_SERVER_ERROR).json({
    error: 'Internal server error',
    referenceId: req.id
  });
});

// ======================
// Server Initialization
// ======================
const server = app.listen(config.port, () => {
  logger.info(`Workflow API running on port ${config.port}`);
  logger.info(`Environment: ${config.env}`);
});

process.on('SIGTERM', () => {
  logger.info('Shutting down server...');
  server.close(async () => {
    await db.destroy();
    redisClient.quit();
    process.exit(0);
  });
});

// ======================
// Usage Example
// ======================
/*
curl -X POST http://localhost:3000/api/workflows \
  -H "Authorization: Bearer <JWT_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "DataProcessing",
    "steps": [{
      "name": "validate-input",
      "action": "validation-service",
      "parameters": {"schema": "v1"},
      "retries": 3
    }]
  }'
*/
