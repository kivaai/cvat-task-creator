import os
import logging
from cvat_sdk import make_client
from cvat_sdk.core.proxies.tasks import ResourceType
import pandas as pd
from typing import List, Dict
import time
from datetime import datetime

# Create logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Generate log filename with timestamp
log_filename = f'logs/cvat_task_creation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

# Configure logging to write to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CVATTaskCreator:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.client = None
        
    def setup_client(self):
        """Initialize CVAT client with credentials"""
        self.client = make_client(
            host=os.getenv('CVAT_HOST', 'https://app.cvat.ai/'),
            credentials=(
                os.getenv('CVAT_USERNAME'),
                os.getenv('CVAT_PASSWORD')
            )
        )
        self.client.organization_slug = 'Kiva'
        logger.info("CVAT client setup complete")
        
    def load_data(self) -> pd.DataFrame:
        """Load and validate CSV data"""
        try:
            data = pd.read_csv(self.csv_path)
            logger.info(f"Loaded CSV with {len(data)} rows")
            return data
        except Exception as e:
            logger.error(f"Error loading CSV data: {str(e)}")
            raise

    def parse_labels(self, labels_str: str) -> List[Dict]:
        """Convert comma-separated labels string to list of label dictionaries"""
        return [{'name': label.strip()} for label in labels_str.split(',')]


    def create_task(self, row):
        """Create a single CVAT task for one image"""
        try:
            # Parse labels for this specific image
            labels = self.parse_labels(row['Labels'])
            
            # Add a Descriptions label with the required text attributes
            labels.append({
                'name': 'Descriptions',
                'attributes': [
                    {
                        'name': 'Title',
                        'mutable': True,
                        'input_type': 'text',
                        'values': [''],
                        'required': True
                    },
                    {
                        'name': 'English_Image_Description',
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
                'labels': labels
            }
            
            task = self.client.tasks.create_from_data(
                spec=task_spec,
                resource_type=ResourceType.REMOTE,
                resources=[row['URL']]
            )
            logger.info(f"Created task {task.id} for image {row['ID']}")
            return task.id
                
        except Exception as e:
            logger.error(f"Error creating task for image {row['ID']}: {str(e)}")
            raise


    def run(self):
        """Main execution method"""
        try:
            self.setup_client()
            data = self.load_data()
            
            total_images = len(data)
            logger.info(f"Starting task creation for {total_images} images")
            
            for idx, row in data.iterrows():
                try:
                    task_id = self.create_task(row)
                    
                    # Progress logging
                    if (idx + 1) % 10 == 0:
                        logger.info(f"Progress: {idx + 1}/{total_images} tasks created")
                    
                    # Rate limiting to avoid overwhelming the API
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Failed to create task for row {idx}: {str(e)}")
                    continue
                
            logger.info("Task creation completed successfully")
            
        except Exception as e:
            logger.error(f"Process failed: {str(e)}")
            raise

def main():
    csv_path = 'Guru_shot_25K_with_labels.csv'
    
    creator = CVATTaskCreator(csv_path)
    creator.run()

if __name__ == "__main__":
    main()