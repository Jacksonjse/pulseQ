import yaml
import subprocess
import kubernetes.client
from kubernetes import config
import asyncio
from typing import Dict, Any

class RunbookExecutor:
    def __init__(self):
        config.load_incluster_config()  # For k8s pod execution
        self.apps_v1 = kubernetes.client.AppsV1Api()
        self.autoscaling_v1 = kubernetes.client.AutoscalingV1Api()
    
    async def execute(self, runbook_file: str, incident: Dict) -> bool:
        """Execute runbook based on incident"""
        with open(runbook_file, 'r') as f:
            runbook = yaml.safe_load(f)
        
        action = runbook['actions'][0]  # First action for MVP
        return await self._execute_action(action, incident)
    
    async def _execute_action(self, action: Dict, incident: Dict) -> bool:
        """Execute single action"""
        action_type = action['type']
        
        if action_type == 'kubectl_scale':
            return await self._scale_deployment(action, incident)
        elif action_type == 'kubectl_restart':
            return await self._restart_deployment(action)
        elif action_type == 'github_actions':
            return await self._trigger_rollback(action)
        else:
            return False
    
    async def _scale_deployment(self, action: Dict, incident: Dict) -> bool:
        """HPA scale-up example"""
        namespace = action.get('namespace', 'default')
        deployment = f"{incident['service']}-deployment"
        
        try:
            # Scale to 3 replicas for CPU saturation
            self.apps_v1.patch_namespaced_deployment(
                name=deployment,
                namespace=namespace,
                body={"spec": {"replicas": 3}}
            )
            print(f"✅ Scaled {deployment} to 3 replicas")
            return True
        except Exception as e:
            print(f"❌ Scale failed: {e}")
            return False
    
    async def _restart_deployment(self, action: Dict) -> bool:
        """Restart deployment"""
        namespace = action.get('namespace', 'default')
        deployment = action['deployment']
        
        cmd = f"kubectl rollout restart deployment/{deployment} -n {namespace}"
        result = subprocess.run(cmd, shell=True, capture_output=True)
        return result.returncode == 0
    
    async def _trigger_rollback(self, action: Dict) -> bool:
        """Trigger GitHub Actions rollback"""
        # Simulate GitHub API call
        print("🚀 Triggering GitHub Actions rollback workflow")
        return True
