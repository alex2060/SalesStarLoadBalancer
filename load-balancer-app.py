from flask import Flask, jsonify
import requests
import time
import json
from datetime import datetime

app = Flask(__name__)

# Configuration - add your actual server URLs here

SERVERS = [
    {'name': 'server1', 'url': 'https://landing1.aimachengine.com', 'weight': 1},
    {'name': 'server2', 'url': 'https://go.aimachengine.com', 'weight': 1},
    {'name': 'server3', 'url': 'https://go1.aimachengine.com', 'weight': 1},
    {'name': 'server4', 'url': 'https://go2.aimachengine.com', 'weight': 1},
    {'name': 'server5', 'url': 'https://1.aimachengine.com', 'weight': 1},
    {'name': 'server6', 'url': 'https://2.aimachengine.com', 'weight': 1},
    {'name': 'server7', 'url': 'https://3.aimachengine.com', 'weight': 1},
    {'name': 'server8', 'url': 'https://4.aimachengine.com', 'weight': 1},
]

def check_server_health(server):
    """Check individual server health and calculate score"""
    try:
        start_time = time.time()
        response = requests.get(f"{server['url']}/health", timeout=5)
        response_time = (time.time() - start_time) * 1000  # ms
        
        if response.status_code == 200:
            health_data = response.json()
            # Calculate score based on response time (lower is better)
            # Score = 100 - response_time, minimum 1
            print(health_data)
            return {
                'name': server['name'],
                'url': server['url'],
                'health': health_data['status'],
                'response_time': round(response_time, 2),
                'score': health_data["score"],
                'status_code': response.status_code,
                'last_checked': datetime.now().isoformat()
            }
        else:
            return {
                'name': server['name'],
                'url': server['url'],
                'health': 'unhealthy',
                'response_time': round(response_time, 2),
                'score': 0,
                'status_code': response.status_code,
                'last_checked': datetime.now().isoformat()
            }
    except Exception as e:
        return {
            'name': server['name'],
            'url': server['url'],
            'health': 'error',
            'response_time': None,
            'score': 0,
            'error': str(e),
            'last_checked': datetime.now().isoformat()
        }

def get_all_server_health():
    """Get health status for all servers"""
    server_health = []
    for server in SERVERS:
        health_data = check_server_health(server)
        server_health.append(health_data)
    return server_health

def get_highest_score_server():
    """Get the server with the highest score"""
    all_servers = get_all_server_health()
    healthy_servers = [s for s in all_servers if s['health'] == 'healthy']
    
    if not healthy_servers:
        return None
    
    return max(healthy_servers, key=lambda x: x['score'])

@app.route('/server')
def upload_endpoint():
    """Return the server with highest score for upload"""
    best_server = get_highest_score_server()
    
    if not best_server:
        return jsonify({
            'error': 'No healthy servers available',
            'server': None,
            'health': 'unavailable'
        }), 503
    
    return jsonify({
        'server': best_server['name'],
        'server_url': best_server['url'],
        'health': best_server['health'],
        'score': best_server['score'],
        'response_time': best_server['response_time'],
        'last_checked': best_server['last_checked']
    })

@app.route('/health')
def health_check():
    """Enhanced health check endpoint"""
    all_servers = get_all_server_health()
    best_server = get_highest_score_server()
    
    return jsonify({
        'status': 'healthy',
        'best_server': best_server,
        'all_servers': all_servers,
        'total_servers': len(all_servers),
        'healthy_servers': len([s for s in all_servers if s['health'] == 'healthy'])
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
