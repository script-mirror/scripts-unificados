import os
import paramiko
import logging
import warnings
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Suppress cryptography deprecation warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("accor_sftp_transfer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('accor_sftp')

# Configuration
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = os.getenv("SFTP_PORT")
SFTP_USERNAME = os.getenv("SFTP_USERNAME")
PRIVATE_KEY_PATH = os.getenv("PRIVATE_KEY_PATH")

# Local directories
LOCAL_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_IN_DIR = os.path.join(LOCAL_BASE_DIR, "in")
LOCAL_OUT_DIR = os.path.join(LOCAL_BASE_DIR, "out")
LOCAL_TO_CONVERT_DIR = os.path.join(LOCAL_BASE_DIR, "to_convert")

# Remote directories
REMOTE_IN_DIR = "IN"
REMOTE_OUT_DIR = "OUT"

# Create local directories if they don't exist
os.makedirs(LOCAL_IN_DIR, exist_ok=True)
os.makedirs(LOCAL_OUT_DIR, exist_ok=True)
os.makedirs(LOCAL_TO_CONVERT_DIR, exist_ok=True)


def connect_to_sftp():
    """Establish connection to the SFTP server using private key"""
    try:
        # Initialize transport
        transport = paramiko.Transport((SFTP_HOST, int(SFTP_PORT)))
        
        # Load private key
        private_key = paramiko.RSAKey(filename=PRIVATE_KEY_PATH)
        
        # Connect to SFTP
        transport.connect(username=SFTP_USERNAME, pkey=private_key)
        
        # Create SFTP client
        sftp = paramiko.SFTPClient.from_transport(transport)
        
        logger.info(f"Connected to SFTP server at {SFTP_HOST}")
        return sftp, transport
    
    except Exception as e:
        logger.error(f"Failed to connect to SFTP server: {str(e)}")
        raise


def convert_excel_to_csv():
    """Convert Excel files in to_convert directory to CSV and place them in out directory"""
    try:
        # List Excel files in to_convert directory
        files = [f for f in os.listdir(LOCAL_TO_CONVERT_DIR) if f.endswith(('.xlsx', '.xls'))]
        
        if not files:
            logger.info("No Excel files to convert.")
            return
        
        # Convert each Excel file to CSV
        for file_name in files:
            excel_path = os.path.join(LOCAL_TO_CONVERT_DIR, file_name)
            csv_name = os.path.splitext(file_name)[0] + '.csv'
            csv_path = os.path.join(LOCAL_OUT_DIR, csv_name)
            
            # Read Excel and convert to CSV with semicolon separator
            df = pd.read_excel(excel_path)
            df.to_csv(csv_path, sep=';', index=False)
            
            logger.info(f"Converted {file_name} to CSV format as {csv_name}")
            
            # Optionally move or delete the processed Excel file
            # os.remove(excel_path)
    
    except Exception as e:
        logger.error(f"Error converting Excel files to CSV: {str(e)}")
        raise


def upload_files_to_accor(sftp):
    """Upload files from local out directory to Accor's IN directory"""
    try:
        # List files in local out directory
        files = os.listdir(LOCAL_OUT_DIR)
        
        if not files:
            logger.info("No files to upload.")
            return
        
        # Upload each file
        for file_name in files:
            local_path = os.path.join(LOCAL_OUT_DIR, file_name)
            remote_path = f"{REMOTE_IN_DIR}/{file_name}"
            
            if os.path.isfile(local_path):
                sftp.put(local_path, remote_path)
                logger.info(f"Uploaded {file_name} to {REMOTE_IN_DIR}")
                
                # Optionally move the local file to a processed directory or delete it
                # os.remove(local_path)
    
    except Exception as e:
        logger.error(f"Error uploading files: {str(e)}")
        raise


def download_files_from_accor(sftp):
    """Download files from Accor's OUT directory to local in directory"""
    try:
        # List files in Accor's OUT directory
        files = sftp.listdir(REMOTE_OUT_DIR)
        
        if not files:
            logger.info("No files to download.")
            return
        
        # Download each file
        for file_name in files:
            local_path = os.path.join(LOCAL_IN_DIR, file_name)
            remote_path = f"{REMOTE_OUT_DIR}/{file_name}"
            
            sftp.get(remote_path, local_path)
            logger.info(f"Downloaded {file_name} from {REMOTE_OUT_DIR}")
            
            # Optionally delete the remote file after download
            # sftp.remove(remote_path)
    
    except Exception as e:
        logger.error(f"Error downloading files: {str(e)}")
        raise


def list_remote_directories(sftp):
    """List contents of remote IN and OUT directories"""
    try:
        logger.info("Listing remote directory contents:")
        logger.info(f"\nIN directory contents:")
        for item in sftp.listdir(REMOTE_IN_DIR):
            logger.info(f"- {item}")
            
        logger.info(f"\nOUT directory contents:")
        for item in sftp.listdir(REMOTE_OUT_DIR):
            logger.info(f"- {item}")
    
    except Exception as e:
        logger.error(f"Error listing remote directories: {str(e)}")


def get_user_choice():
    """Ask user for which operation they want to perform"""
    print("\nACCOR SFTP Operations Menu:")
    print("1. List remote directories contents")
    print("2. Transfer files (upload and download)")
    print("3. Perform both operations")
    print("4. Only convert Excel files to CSV")
    
    while True:
        try:
            choice = int(input("\nEnter your choice (1-4): "))
            if 1 <= choice <= 4:
                return choice
            else:
                print("Invalid choice. Please enter a number between 1 and 4.")
        except ValueError:
            print("Invalid input. Please enter a number.")


def main():
    """Main function to orchestrate SFTP operations"""
    sftp = None
    transport = None
    
    try:
        # Get user choice
        choice = get_user_choice()
        logger.info(f"User selected option {choice}")
        
        # Convert Excel files to CSV first
        if choice in [2, 3, 4]:
            convert_excel_to_csv()
        
        # If choice is 4, we're done
        if choice == 4:
            logger.info("Excel to CSV conversion completed.")
            return
        
        # Connect to SFTP
        sftp, transport = connect_to_sftp()
        
        # Perform operations based on user choice
        if choice == 1:
            # Only list directories
            list_remote_directories(sftp)
        elif choice == 2:
            # Only transfer files
            upload_files_to_accor(sftp)
            download_files_from_accor(sftp)
            logger.info("File transfers completed successfully.")
        elif choice == 3:
            # Do both operations
            upload_files_to_accor(sftp)
            download_files_from_accor(sftp)
            list_remote_directories(sftp)
        
        logger.info("SFTP operations completed successfully.")
        
    except Exception as e:
        logger.error(f"Error in SFTP operations: {str(e)}")
        
    finally:
        # Close connections
        if sftp:
            sftp.close()
        if transport:
            transport.close()
        logger.info("SFTP connection closed.")


if __name__ == "__main__":
    logger.info("Starting Accor SFTP transfer process")
    main()
