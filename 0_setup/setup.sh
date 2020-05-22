#!/bin/bash

sudo su postgres -c "psql < <(echo 'DROP DATABASE cpdp ; CREATE DATABASE cpdp ;')"

psql -U cpdp -d cpdp < setup_database.sql

python3 setup_batches.py
