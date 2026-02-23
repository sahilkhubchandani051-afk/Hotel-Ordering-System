import os
import sys
import asyncio
import json
import sqlite3
try:
    import psycopg2
    from psycopg2 import extras
except ImportError:
    psycopg2 = None
    extras = None
import logging
import random
import string
import re
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict
from app import utils
from app.utils import number_to_words
try:
    from deep_translator import GoogleTranslator
except ImportError:
    GoogleTranslator = None

# Grok AI integration (optional)
try:
    import requests
except ImportError:
    requests = None
