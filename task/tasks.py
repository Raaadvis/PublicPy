from robocorp.tasks import task
from robocorp import browser
from bs4 import BeautifulSoup
import gspread
from gspread.exceptions import APIError
import time
import re

# Browser settings
browser.configure(headless=True)

# Global variables
BASE_URL = "https://antmedineslenteles.com/category/aml-sventiniai-meniu/page/"
SPREADSHEET_NAME= "AML_Recipes_sh"
GOOGLE_API_CREDS_FILE= "google_api_credentals.json"

@task
def aml_recipe_task():

    all_recipes = [] 
    all_recipes = get_recipe_urls()
    for recipe_category in all_recipes:
        get_recipe_details(recipe_category)
    upload_to_google_sheets(all_recipes, SPREADSHEET_NAME)

def get_recipe_urls():
    """Loop through pages and append URLs of all recipe groups"""
    recipes = []

    # Loop through web pages
    for i in range(1,99):
        browser.goto(BASE_URL + str(i))
        page = browser.page()
        category_title_selector = ".wpr-grid-item-title.wpr-grid-item-align-center"

        # Check if recipes exist
        recipes_visible = page.query_selector(category_title_selector)

        if recipes_visible:
            # Extract all recipe categories
            categories = page.query_selector_all(category_title_selector)

            for category in categories:
                # Extract URL
                href_value = category.query_selector("a").get_attribute("href")

                recipes.append({
                    "group_name": "",
                    "group_url": href_value,
                    "text_bodies": [],
                })
        else:
            """No more pages to scrape"""  
            break
    return recipes

def get_recipe_details(recipe):
    """Open recipe URL, fetch group name and html bodies to recipe row"""
    page = browser.goto(recipe["group_url"])

    # Retrieve list of recipes using selectors and RegEx
    recipe["group_name"] = page.query_selector(".wpr-post-title").inner_text()
    full_html = page.locator(".wpr-post-content").inner_html()
    separators = ['text-align:center', 'text-align-center']
    pattern = '|'.join(map(re.escape, separators))
    list_of_recipes = re.split(pattern, full_html)[1:]
    
     # Use Beautiful Soup to parse the HTML content
    for recipe_text in list_of_recipes: 
        soup = BeautifulSoup(recipe_text, 'html.parser')
        text = soup.get_text(separator=' ', strip=True)

        # Find the first instance of '>'
        start_index = text.find('>') + 1  # Add 1 to exclude the symbol itself
        if start_index > 0:
            text = text[start_index:len(text)]

        # Find the first instance of '<'
        end_index = text.find('<')
        if end_index > 0:
            text = text[0:end_index]

        text = text.strip()
        # Exclude garbage content
        if text.lower().startswith("pirkini"):
            continue

        # Add the inner_text to the recipe dictionary
        recipe["text_bodies"].append(text)

def upload_to_google_sheets(all_recipes, spreadsheet):
    """Uploads recipes to google sheet"""

    # Authenticate with Google Sheets using a service account
    gc = gspread.service_account(filename=GOOGLE_API_CREDS_FILE)
    
    # Get spreadsheets worksheet handle
    sh = gc.open(spreadsheet)
    worksheet = sh.sheet1

    # Refresh sheet
    worksheet.clear()
    row_index = 1

    # Function with error handling which populates rows with recipe 
    def update_cell_with_retry(worksheet, row, col, value, retries=10, delay=5):
        for attempt in range(retries):
            try:
                worksheet.update_cell(row, col, value)
                return
            except APIError as e:
                if e.response.status_code == 429:
                    print(f"Quota exceeded. Retrying in {delay} seconds...")
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    raise
        raise Exception("Max retries exceeded")

    # Populate rows with recipe
    for recipe_category in all_recipes:
        update_cell_with_retry(worksheet, row_index, 1, recipe_category["group_name"])

        for i, recipe in enumerate(recipe_category["text_bodies"]):
            row_index += 1
            update_cell_with_retry(worksheet, row_index, 1, recipe)
        row_index += 2