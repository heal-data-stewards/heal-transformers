#!/bin/bash

python3 -m venv .venv
source ./.venv/bin/activate

pip install --upgrade pip

if [ -f "requirements.txt" ]; then
    echo "Installing dependencies from requirements.txt..."
    pip install -r requirements.txt
else
    echo "requirements.txt not found."
fi

echo "To activate the virtual environment, run: source ./.venv/bin/activate"
