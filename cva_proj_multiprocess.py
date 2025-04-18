import os
import logging
from cvat_sdk import make_client
from cvat_sdk.core.proxies.tasks import ResourceType
import pandas as pd
from typing import List, Dict
import time
from datetime import datetime
from multiprocessing import Pool, cpu_count
from functools import partial
import random
from cvat_sdk.models import PatchedTaskWriteRequest
import csv


# Logging setup
if not os.path.exists('logs'):
    os.makedirs('logs')

log_filename = f'logs/cvat_task_creation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CVATTaskCreator:
    def __init__(self, csv_path: str, assignee_email: str = "ak294208@gmail.com"):
        self.csv_path = csv_path
        self.assignee_email = assignee_email

    def setup_client(self):
        client = make_client(
            host=os.getenv('CVAT_HOST', 'https://app.cvat.ai/'),
            credentials=(
                os.getenv('CVAT_USERNAME'),
                os.getenv('CVAT_PASSWORD')
            )
        )
        client.organization_slug = 'Kiva'
        return client

    def create_single_task(self, row_data):
        """Create a single task - to be run in parallel"""
        try:
            client = self.setup_client()  # Each process needs its own client
            row = pd.Series(row_data)
            
            labels = self.parse_labels(row['Labels'])
            labels.append({
                'name': 'description',
                'attributes': [
                    {
                        'name': 'Title',
                        'mutable': True,
                        'input_type': 'text',
                        'values': [''],
                        'required': True
                    },
                    {
                        'name': 'Image_Description',
                        'mutable': True,
                        'input_type': 'text',
                        'values': [''],
                        'required': True
                    },
                    {
                        'name': 'Scene_Description',
                        'mutable': True,
                        'input_type': 'text',
                        'values': [''],
                        'required': True
                    }
                ]
            })
            
            task_spec = {
                'name': f"Segmentation_{row['ID']}",
                'labels': labels,
            }           
            
            # Create the task with the assignee already set
            task = client.tasks.create_from_data(
                spec=task_spec,
                resource_type=ResourceType.REMOTE,
                resources=[row['URL']]
            )
            
            logger.info(f"Created task {task.id} for image {row['ID']}")

            # log the task in the csv file

            # Sleep a bit to avoid overwhelming the API
            time.sleep(1)
            
            return {'success': True, 'id': row['ID'], 'task_id': task.id}
            
        except Exception as e:
            logger.error(f"Error creating task for image {row['ID']}: {str(e)}")
            return {'success': False, 'id': row['ID'], 'error': str(e)}

    def parse_labels(self, labels_str: str) -> List[Dict]:
        return [{'name': label.strip()} for label in labels_str.split(',')]

    def run(self):
        try:
            # Load data
            data = pd.read_csv(self.csv_path)
            logger.info(f"Loaded CSV with {len(data)} rows")
            
            # Convert DataFrame to list of dictionaries for multiprocessing
            data_dicts = data.to_dict('records')
            
            # Calculate number of processes
            num_processes = min(cpu_count(), 4)  # Limit to 4 processes
            logger.info(f"Starting task creation with {num_processes} processes")
            
            # Create tasks in parallel
            with Pool(num_processes) as pool:
                results = pool.map(self.create_single_task, data_dicts)
            
            # Process results
            successes = [r for r in results if r['success']]
            failures = [r for r in results if not r['success']]
            
            logger.info(f"Task creation completed. Successfully created: {len(successes)}, Failed: {len(failures)}")
            
            # Save successful task details to a CSV file
            if successes:
                success_csv = f'logs/successes_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                with open(success_csv, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=['Task ID', 'Task URL'])
                    writer.writeheader()
                    for success in successes:
                        writer.writerow({
                            'Task ID': success['task_id'],
                            'Task URL': f"https://app.cvat.ai/tasks/{success['task_id']}"
                        })
                logger.info(f"Successful task details written to {success_csv}")
            
            # Log failures to a separate file
            if failures:
                failure_log = f'logs/failures_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
                with open(failure_log, 'w') as f:
                    for failure in failures:
                        f.write(f"ID: {failure['id']}, Error: {failure.get('error', 'Unknown error')}\n")
                logger.info(f"Failed task details written to {failure_log}")
            
        except Exception as e:
            logger.error(f"Process failed: {str(e)}")
            raise

def main():
    csv_path = 'external_batch_02_mini.csv'
    assignee_email = "ak294208@gmail.com"  # Replace with the annotator's email
    creator = CVATTaskCreator(csv_path, assignee_email)
    creator.run()

if __name__ == "__main__":
    main()