import os
import logging
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from dotenv import load_dotenv
from pyrfc import Connection, RFCError, ABAPApplicationError, ABAPRuntimeError, LogonError, CommunicationError

# Environment Configuration
load_dotenv()

class SAPConnectionPool:
    """Managed connection pool for SAP RFC connections"""
    
    def __init__(self, max_connections: int = 5):
        self.pool: List[Connection] = []
        self.max_connections = max_connections
        self._config = {
            'ashost': os.getenv('SAP_ASHOST'),
            'sysnr': os.getenv('SAP_SYSNR'),
            'client': os.getenv('SAP_CLIENT'),
            'user': os.getenv('SAP_USER'),
            'passwd': os.getenv('SAP_PASSWD'),
            'lang': os.getenv('SAP_LANG', 'EN'),
        }
        
        self._validate_config()
        self.logger = self._configure_logger()
        
    def _validate_config(self):
        """Validate essential connection parameters"""
        required = ['ashost', 'sysnr', 'client', 'user', 'passwd']
        missing = [param for param in required if not self._config.get(param)]
        if missing:
            raise ValueError(f"Missing SAP configuration: {', '.join(missing)}")

    def _configure_logger(self):
        """Configure application logging"""
        logger = logging.getLogger('SAP-RFC')
        logger.setLevel(logging.DEBUG)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _create_connection(self) -> Connection:
        """Establish new SAP RFC connection with error handling"""
        try:
            return Connection(**self._config)
        except (RFCError, LogonError, CommunicationError) as e:
            self.logger.error(f"Connection failed: {str(e)}")
            raise
        except Exception as e:
            self.logger.critical(f"Unexpected connection error: {str(e)}")
            raise

    def get_connection(self) -> Connection:
        """Acquire connection from pool or create new one"""
        if self.pool:
            return self.pool.pop()
            
        if len(self.pool) < self.max_connections:
            return self._create_connection()
            
        raise RFCError("Connection pool exhausted")

    def release_connection(self, conn: Connection):
        """Return connection to the pool"""
        if len(self.pool) < self.max_connections:
            self.pool.append(conn)
        else:
            conn.close()
            self.logger.debug("Closed excess connection")

class SAPRFCClient:
    """High-level SAP RFC client with transactional support"""
    
    def __init__(self, connection_pool: SAPConnectionPool):
        self.pool = connection_pool
        self.transaction_connections = {}

    def _get_transaction_conn(self, tid: str) -> Connection:
        """Retrieve dedicated transaction connection"""
        if tid not in self.transaction_connections:
            self.transaction_connections[tid] = self.pool.get_connection()
        return self.transaction_connections[tid]

    def execute(
        self,
        function_name: str,
        parameters: Dict[str, Any],
        transactional: bool = False,
        tid: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute RFC call with comprehensive error handling"""
        conn = None
        try:
            if transactional:
                if not tid:
                    raise ValueError("Transaction ID required for transactional calls")
                conn = self._get_transaction_conn(tid)
            else:
                conn = self.pool.get_connection()

            result = conn.call(function_name, **parameters)
            return self._parse_result(result)

        except ABAPApplicationError as e:
            self.pool.logger.error(f"ABAP error in {function_name}: {str(e)}")
            raise
        except ABAPRuntimeError as e:
            self.pool.logger.error(f"Runtime error in {function_name}: {str(e)}")
            raise
        except CommunicationError as e:
            self.pool.logger.error(f"Communication error: {str(e)}")
            raise
        except RFCError as e:
            self.pool.logger.error(f"RFC error: {str(e)}")
            raise
        except Exception as e:
            self.pool.logger.critical(f"Unexpected error: {str(e)}")
            raise
        finally:
            if conn and not transactional:
                self.pool.release_connection(conn)

    def start_transaction(self) -> str:
        """Initiate stateful transaction context"""
        tid = f"TID-{datetime.now().timestamp()}-{os.getpid()}"
        _ = self._get_transaction_conn(tid)  # Reserve connection
        return tid

    def commit_transaction(self, tid: str):
        """Commit transaction and release connection"""
        try:
            conn = self.transaction_connections.pop(tid)
            conn.call('BAPI_TRANSACTION_COMMIT')
        finally:
            self.pool.release_connection(conn)

    def rollback_transaction(self, tid: str):
        """Rollback transaction and release connection"""
        try:
            conn = self.transaction_connections.pop(tid)
            conn.call('BAPI_TRANSACTION_ROLLBACK')
        finally:
            self.pool.release_connection(conn)

    def _parse_result(self, result: Dict) -> Dict:
        """Parse and normalize RFC response"""
        parsed = {}
        for key, value in result.items():
            if isinstance(value, dict) and 'RFC_NOTICE' in value:
                parsed[key] = self._handle_structure(value)
            elif isinstance(value, list):
                parsed[key] = [self._parse_result(item) for item in value]
            else:
                parsed[key] = value
        return parsed

    def _handle_structure(self, structure: Dict) -> Dict:
        """Process complex ABAP structures"""
        return {k: v for k, v in structure.items() if not k.startswith('RFC')}

# Example Usage
if __name__ == "__main__":
    # Initialize connection pool
    pool = SAPConnectionPool(max_connections=3)
    client = SAPRFCClient(pool)

    # Execute simple RFC call
    try:
        result = client.execute('STFC_CONNECTION', {'REQUTEXT': 'Hello SAP'})
        print(json.dumps(result, indent=2))
    except RFCError as e:
        print(f"RFC execution failed: {str(e)}")

    # Transactional example
    tid = client.start_transaction()
    try:
        # Execute multiple RFCs in transaction
        client.execute('BAPI_MATERIAL_GETLIST', 
                      {"MATERIAL": "MAT-001"}, 
                      transactional=True, 
                      tid=tid)
        client.execute('BAPI_TRANSACTION_COMMIT', {}, transactional=True, tid=tid)
        client.commit_transaction(tid)
    except Exception as e:
        client.rollback_transaction(tid)
        print(f"Transaction failed: {str(e)}")
