"""
Load Balancer Application
A high-performance server health monitoring and routing service
"""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple
from functools import lru_cache
import logging

from flask import Flask, jsonify, Response
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Server health status enumeration"""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    ERROR = "error"


@dataclass
class ServerConfig:
    """Server configuration data class"""
    name: str
    url: str
    weight: int = 1
    
    def __hash__(self):
        return hash((self.name, self.url))


@dataclass
class ServerHealth:
    """Server health information data class"""
    name: str
    url: str
    health: HealthStatus
    response_time: Optional[float]
    score: float
    status_code: Optional[int] = None
    error: Optional[str] = None
    last_checked: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'name': self.name,
            'url': self.url,
            'health': self.health.value,
            'response_time': self.response_time,
            'score': self.score,
            'status_code': self.status_code,
            'error': self.error,
            'last_checked': self.last_checked
        }


class HealthChecker:
    """Handles server health checking with caching and connection pooling"""
    
    def __init__(self, timeout: int = 5, max_retries: int = 2, cache_ttl: int = 10):
        self.timeout = timeout
        self.cache_ttl = cache_ttl
        self.cache: Dict[str, Tuple[ServerHealth, float]] = {}
        self.session = self._create_session(max_retries)
        self.executor = ThreadPoolExecutor(max_workers=10)
    
    def _create_session(self, max_retries: int) -> requests.Session:
        """Create a requests session with connection pooling and retry logic"""
        session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=retry_strategy
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def _is_cache_valid(self, server_url: str) -> bool:
        """Check if cached data is still valid"""
        if server_url not in self.cache:
            return False
        _, timestamp = self.cache[server_url]
        return (time.time() - timestamp) < self.cache_ttl
    
    def check_server(self, server: ServerConfig) -> ServerHealth:
        """Check individual server health with caching"""
        # Check cache first
        if self._is_cache_valid(server.url):
            return self.cache[server.url][0]
        
        try:
            start_time = time.time()
            response = self.session.get(
                f"{server.url}/health",
                timeout=self.timeout,
                allow_redirects=False
            )
            response_time = (time.time() - start_time) * 1000  # ms
            
            if response.status_code == 200:
                try:
                    health_data = response.json()
                    health = ServerHealth(
                        name=server.name,
                        url=server.url,
                        health=HealthStatus.HEALTHY,
                        response_time=round(response_time, 2),
                        score=health_data.get("score", self._calculate_score(response_time)),
                        status_code=response.status_code
                    )
                except (ValueError, KeyError) as e:
                    logger.warning(f"Invalid health response from {server.name}: {e}")
                    health = ServerHealth(
                        name=server.name,
                        url=server.url,
                        health=HealthStatus.UNHEALTHY,
                        response_time=round(response_time, 2),
                        score=0,
                        status_code=response.status_code,
                        error="Invalid health response"
                    )
            else:
                health = ServerHealth(
                    name=server.name,
                    url=server.url,
                    health=HealthStatus.UNHEALTHY,
                    response_time=round(response_time, 2),
                    score=0,
                    status_code=response.status_code
                )
                
        except requests.RequestException as e:
            logger.error(f"Error checking {server.name}: {e}")
            health = ServerHealth(
                name=server.name,
                url=server.url,
                health=HealthStatus.ERROR,
                response_time=None,
                score=0,
                error=str(e)
            )
        
        # Update cache
        self.cache[server.url] = (health, time.time())
        return health
    
    def check_all_servers_parallel(self, servers: List[ServerConfig]) -> List[ServerHealth]:
        """Check all servers in parallel"""
        futures = [self.executor.submit(self.check_server, server) for server in servers]
        results = [future.result() for future in futures]
        return results
    
    @staticmethod
    def _calculate_score(response_time: float) -> float:
        """Calculate score based on response time (lower is better)"""
        if response_time < 100:
            return 100
        elif response_time < 500:
            return 100 - (response_time - 100) * 0.2
        else:
            return max(1, 100 - response_time * 0.1)
    
    def __del__(self):
        """Cleanup resources"""
        self.session.close()
        self.executor.shutdown(wait=False)


class LoadBalancer:
    """Main load balancer logic"""
    
    def __init__(self, servers: List[ServerConfig], health_checker: HealthChecker):
        self.servers = servers
        self.health_checker = health_checker
    
    def get_all_server_health(self) -> List[ServerHealth]:
        """Get health status for all servers"""
        return self.health_checker.check_all_servers_parallel(self.servers)
    
    def get_best_server(self) -> Optional[ServerHealth]:
        """Get the server with the highest score"""
        all_servers = self.get_all_server_health()
        healthy_servers = [
            s for s in all_servers 
            if s.health == HealthStatus.HEALTHY
        ]
        
        if not healthy_servers:
            logger.warning("No healthy servers available")
            return None
        
        return max(healthy_servers, key=lambda x: x.score)


# Initialize Flask app
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

# Server configuration
SERVERS = [
    ServerConfig('server1', 'https://landing1.aimachengine.com'),
    ServerConfig('server2', 'https://go.aimachengine.com'),
    ServerConfig('server3', 'https://go1.aimachengine.com'),
    ServerConfig('server4', 'https://go2.aimachengine.com'),
    ServerConfig('server5', 'https://1.aimachengine.com'),
    ServerConfig('server6', 'https://2.aimachengine.com'),
    ServerConfig('server7', 'https://3.aimachengine.com'),
    ServerConfig('server8', 'https://4.aimachengine.com'),
]

# Initialize components
health_checker = HealthChecker(timeout=3, cache_ttl=10)
load_balancer = LoadBalancer(SERVERS, health_checker)


@app.route('/server')
def upload_endpoint() -> Tuple[Response, int]:
    """Return the server with highest score for upload"""
    try:
        best_server = load_balancer.get_best_server()
        
        if not best_server:
            return jsonify({
                'error': 'No healthy servers available',
                'server': None,
                'health': 'unavailable'
            }), 503
        
        return jsonify({
            'server': best_server.name,
            'server_url': best_server.url,
            'health': best_server.health.value,
            'score': best_server.score,
            'response_time': best_server.response_time,
            'last_checked': best_server.last_checked
        }), 200
        
    except Exception as e:
        logger.error(f"Error in upload_endpoint: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/health')
def health_check() -> Tuple[Response, int]:
    """Enhanced health check endpoint"""
    try:
        all_servers = load_balancer.get_all_server_health()
        best_server = load_balancer.get_best_server()
        
        healthy_count = sum(
            1 for s in all_servers 
            if s.health == HealthStatus.HEALTHY
        )
        
        return jsonify({
            'status': 'healthy' if healthy_count > 0 else 'degraded',
            'best_server': best_server.to_dict() if best_server else None,
            'all_servers': [s.to_dict() for s in all_servers],
            'total_servers': len(all_servers),
            'healthy_servers': healthy_count,
            'timestamp': datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Error in health_check: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500


@app.route('/servers/<server_name>/health')
def individual_server_health(server_name: str) -> Tuple[Response, int]:
    """Get health status for a specific server"""
    try:
        server = next(
            (s for s in SERVERS if s.name == server_name),
            None
        )
        
        if not server:
            return jsonify({'error': 'Server not found'}), 404
        
        health = health_checker.check_server(server)
        return jsonify(health.to_dict()), 200
        
    except Exception as e:
        logger.error(f"Error checking server {server_name}: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    logger.error(f"Internal server error: {error}")
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    # Production configuration
    app.run(
        host='0.0.0.0',
        port=10000,
        debug=False,
        threaded=True
    )