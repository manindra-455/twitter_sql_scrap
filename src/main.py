import csv
import pandas as pd
import random
import subprocess
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import mysql.connector

# List of user-agents to choose from
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
]

# Function to get a random user-agent
def get_random_user_agent():
    return random.choice(user_agents)

# Configure Selenium options with a random user-agent
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run in headless mode (optional)
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument(f"user-agent={get_random_user_agent()}")

# Initialize the WebDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Database connection details
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "uttam",
    "database": "TwitterData",
}

# Input file path
input_file = 'twittersql/data/twitter_links.csv'  # Input file with profile URLs

# Function to create database and table
def setup_database_and_table():
    print("Creating database and table...")
    try:
        connection = mysql.connector.connect(
            host=db_config["host"],
            user=db_config["user"],
            password=db_config["password"]
        )
        cursor = connection.cursor()
        
        # Create database if it doesn't exist
        cursor.execute("CREATE DATABASE IF NOT EXISTS TwitterData")
        connection.database = db_config["database"]
        
        # Create table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS Profiles (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ProfileURL VARCHAR(255) NOT NULL,
            Bio TEXT,
            FollowingCount VARCHAR(50),
            FollowersCount VARCHAR(50),
            Location VARCHAR(255),
            Website VARCHAR(255)
        )
        """
        cursor.execute(create_table_query)
        print("Database and table setup completed.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        exit(1)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Define function to scrape a Twitter profile
def scrape_profile(url):
    driver.get(url)
    wait = WebDriverWait(driver, 5)

    try:
        # Bio
        bio = wait.until(EC.visibility_of_element_located((By.XPATH, '//div[@data-testid="UserDescription"]//span'))).text
    except:
        try:
            # Alternative path for Bio
            bio = wait.until(EC.visibility_of_element_located((By.XPATH, '//div[@data-testid="UserDescription"]/div/span'))).text
        except:
            bio = None

    try:
        # Following Count
        following_count = wait.until(EC.visibility_of_element_located((By.XPATH, '//a[contains(@href, "/following")]/span[1]/span'))).text
    except:
        try:
            # Alternative path for Following Count
            following_count = wait.until(EC.visibility_of_element_located((By.XPATH, '//a[@href="/following"]/span[1]'))).text
        except:
            following_count = None

    try:
        # Followers Count
        followers_count = wait.until(EC.visibility_of_element_located((By.XPATH, '//a[contains(@href, "/verified_followers")]/span[1]/span'))).text
    except:
        try:
            # Alternative path for Followers Count
            followers_count = wait.until(EC.visibility_of_element_located((By.XPATH, '//a[@href="/verified_followers"]/span[1]'))).text
        except:
            followers_count = None

    try:
        # Location
        location = wait.until(EC.visibility_of_element_located((By.XPATH, '//span[@data-testid="UserLocation"]'))).text
    except:
        try:
            # Alternative path for Location
            location = wait.until(EC.visibility_of_element_located((By.XPATH, '//div[@data-testid="UserProfileHeader_Items"]//span[@data-testid="UserLocation"]'))).text
        except:
            location = None

    try:
        # Website
        website = wait.until(EC.visibility_of_element_located((By.XPATH, '//a[@data-testid="UserUrl"]'))).text
    except:
        try:
            # Alternative path for Website
            website = wait.until(EC.visibility_of_element_located((By.XPATH, '//a[contains(@href, "https://t.co/") and @data-testid="UserUrl"]'))).text
        except:
            website = None

    return {
        "Bio": bio,
        "Following Count": following_count,
        "Followers Count": followers_count,
        "Location": location,
        "Website": website
    }

# Insert profile data into the database
def save_to_database(profile_info):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        insert_query = """
        INSERT INTO Profiles (ProfileURL, Bio, FollowingCount, FollowersCount, Location, Website)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            profile_info['ProfileURL'],
            profile_info['Bio'],
            profile_info['Following Count'],
            profile_info['Followers Count'],
            profile_info['Location'],
            profile_info['Website']
        ))
        connection.commit()
        print(f"Data for {profile_info['ProfileURL']} saved to database.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Main execution
if __name__ == "__main__":
    # Step 1: Setup the database and table
    setup_database_and_table()

    # Step 2: Read the input file
    profile_links = pd.read_csv(input_file, header=None, names=['ProfileURL'])

    # Step 3: Loop through each profile link and scrape data
    for index, row in profile_links.iterrows():
        url = row['ProfileURL']
        print(f"Scraping profile: {url}")
        profile_info = scrape_profile(url)
        profile_info['ProfileURL'] = url
        save_to_database(profile_info)

        # Print profile's data to the terminal
        print(f"Profile URL: {url}")
        print(f"Bio: {profile_info['Bio']}")
        print(f"Following Count: {profile_info['Following Count']}")
        print(f"Followers Count: {profile_info['Followers Count']}")
        print(f"Location: {profile_info['Location']}")
        print(f"Website: {profile_info['Website']}")
        print("-" * 50)  # Separator for readability

    # Close the WebDriver
    driver.quit()
    print("Scraping complete.")
