# CVAT Task Creator

A Python-based tool for automatically creating CVAT tasks from a CSV file using multiprocessing.

## Features

- Bulk creation of CVAT tasks from CSV data
- Multiprocessing support for faster task creation
- Configurable logging system
- Error handling and failure logging
- Support for custom labels and attributes

## Installation

```bash
# Clone the repository
git clone https://github.com/your-org/cvat-task-creator.git
cd cvat-task-creator

# Create and activate virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  

# Install the requirements
pip install -r requirements.txt
```

## Configuration: 

Export environmetal variables MUST BE ADDED BEFORE RUNNING THE CODE
```
export CVAT_HOST=https://app.cvat.ai/
export CVAT_USERNAME='your_username'
export CVAT_PASSWORD='your_password'
```

2. Prepare your CSV file with the following columns:
- ID: Unique identifier for each task
- URL: Remote URL of the image
- Labels: Comma-separated list of labels

## Usage

```python
python cva_proj_multiprocess.py
```
