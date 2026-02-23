from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from confluent_kafka import Consumer
import httpx
import asyncio
import yaml
import json
from typing import Dict, Any
import logging
from datetime import datetime

app = FastAPI(title="SmartOps Orchestrator")
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Kafka consumer setup
consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'orchestrator',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['raw-alerts']) #listens to raw alerts from kafka

class Alert(BaseModel): #structure for incoming kafka alerts
    service: str
    severity: str
    metrics: Dict[str, float]
    timestamp: str

class Incident(BaseModel): #this is for after ai processing
    id: str
    service: str
    anomaly_score: float
    root_cause: str
    confidence: float
    recommended_action: str

@app.on_event("startup")
async def startup_event():
    """Start Kafka consumer loop"""
    asyncio.create_task(kafka_consumer_loop())

async def kafka_consumer_loop():
    """Main event loop: Kafka → AI → Decision → Action"""
    while True:
        msg = consumer.poll(1.0)
        if msg and not msg.error():
            alert = json.loads(msg.value().decode('utf-8'))
            log.info(f"Processing alert: {alert['service']}")
            
            incident = await process_with_ai(alert) #calls AI
            
            if incident and incident.confidence > 0.8:  #decision taking block
                await execute_runbook(incident)
            elif incident:
                await log_incident(incident, status="human_review")
        
        await asyncio.sleep(0.1)

#AI PROCESSING
async def process_with_ai(alert: Dict) -> Incident:
    """Call anomaly + RCA services"""
    async with httpx.AsyncClient() as client:

        # Call anomaly service - ML service
        anomaly_resp = await client.post(
            "http://localhost:8001/anomaly/score", 
            json={"metrics": alert["metrics"], "service": alert["service"]}
        )
        anomaly = anomaly_resp.json()
        
        if not anomaly["is_anomalous"]:
            return None
        
        # Call RCA service - ROOT CAUSE ANALYSIS
        rca_resp = await client.post(
            "http://localhost:8002/rca/predict",
            json={"alerts": [alert], "metrics": alert["metrics"]}
        )
        rca = rca_resp.json()
        
        return Incident(
            id=f"inc_{int(datetime.now().timestamp())}",
            service=alert["service"],
            anomaly_score=anomaly["anomaly_score"],
            root_cause=rca["root_cause"],
            confidence=rca["confidence"],
            recommended_action=rca["recommended_action"]
        )


#INCIDENT RESPONSE
async def execute_runbook(incident: Incident):
    """Execute appropriate runbook"""
    runbook_map = { #the template for root cause and its respective notebook
        "cpu_saturation": "runbooks/cpu_saturation.yml",
        "oom_killed": "runbooks/oom_restart.yml",
        "error_rate_spike": "runbooks/error_rollback.yml",
        "pod_crashloop": "runbooks/crashloop_restart.yml",
        "db_pool_exhaustion": "runbooks/db_pool.yml"
    }
    
    runbook_file = runbook_map.get(incident.root_cause)
    if runbook_file:
        from executor.runbook_executor import RunbookExecutor
        executor = RunbookExecutor()
        success = await executor.execute(runbook_file, incident.dict())
        await log_incident(incident, status="auto_fixed" if success else "failed")
    else:
        await log_incident(incident, status="no_runbook")

async def log_incident(incident: Incident, status: str):
    """Log to Postgres"""
    async with httpx.AsyncClient() as client:
        await client.post("http://localhost:8003/incidents", json={
            "incident": incident.dict(),
            "status": status,
            "timestamp": datetime.now().isoformat()
        })

# API for testing runbooks manually
@app.post("/test-runbook/{runbook_name}")
async def test_runbook(runbook_name: str, incident: Incident):
    """Manual runbook trigger for testing"""
    await execute_runbook(incident)
    return {"status": "triggered", "runbook": runbook_name}

#NOTE FOR ML:
#the root cause should match the runbook_map'name
#for example, if the root cause is "cpu_saturation", the corresponding runbook should be "runbooks/cpu_saturation.yml"


