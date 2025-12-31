############### Create python virtual environment Method 1: as https://github.com/stajie99/funding-rate-arb-analysis/tree/main

# pip install poetry
# # Verify poetry installation
# poetry --version
# # Poetry helps you declare, manage and install dependencies of Python projects, ensuring you have the right stack everywhere.

# # # Configure poetry to create virtual environments in your project folder
# # poetry config virtualenvs.in-project true

# # # Initialize poetry
# # poetry init

# # # Create virtual environment and install dependencies
# # poetry install
# # # After poetry install, Poetry creates the virtual environment in a system-specific location, 
# not in your project folder by default. Here's how to find and manage it.

# 1. Show all poetry virtual environments
poetry env list

# 2. Show info about current environment
poetry env info

# 3. Get the full path
poetry env info --path

# Configure Poetry to create venv in project
poetry config virtualenvs.in-project true

# Now when you run poetry install, it creates .venv/ in your project
poetry install

################ do not run below: Method 2
# # Create virtual environment
# python -m venv venv

# # Activate it
# venv\Scripts\activate.bat

# # To deactivate
# deactivate