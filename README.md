# GeoBoundaries Data Upload

This repository contains a Python script designed to upload data files to the Harvard Dataverse using the DVUploader tool. The script creates zip files of data chunks from the `gbOpen` directory in the geoBoundaries repository and uploads them to the specified dataset on the Harvard Dataverse.

## Overview

The script performs the following main steps:

1. **Data Chunking**:
   - The script scans the `gbOpen` directory for data files organized by administrative levels (ADM0, ADM1, ADM2, etc.).
   - It creates zip files containing chunks of data within the specified size limit.

2. **File Upload**:
   - The DVUploader tool is used to upload each zip file chunk to the specified dataset on the Harvard Dataverse.
   - The script executes DVUploader commands in the terminal for each zip file chunk.

## Requirements

- Python 3
- DVUploader tool (Java)

## Usage

1. Clone this repository to your local machine:

   ```bash
   git clone <repository-url>
