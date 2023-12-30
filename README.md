# Pinecone Utility GUI

![image](https://github.com/KernAlan/pineconegui/assets/63753020/ab675ce8-f546-4951-b7d7-ec2e31f9ad43)

## Overview
Pinecone Utility GUI is a Python application designed to streamline the process of embedding textual data using OpenAI's models and managing these embeddings with Pinecone, a vector database. It provides a graphical interface for processing CSV files, generating embeddings, and performing various operations within Pinecone.

## Features
- **CSV Processing**: Read and process CSV files with options to select specific columns for embedding and metadata.
- **Text Embedding**: Leverage OpenAI models for creating embeddings from textual data.
- **Pinecone Integration**: Upload, fetch, and manage your embeddings directly within Pinecone.
- **Intuitive GUI**: Easy-to-use interface, making it accessible for both technical and non-technical users.

## Installation

To install and run the Pinecone Utility GUI, follow these steps:

1. **Clone the Repository**:
2. **Install Dependencies**:
```
pip install -r requirements.txt
```
3. **Environment Variables**: Create a `.env` file in the root directory and add your OpenAI and Pinecone API keys:
```
OPENAI_API_KEY=your_openai_api_key
PINECONE_API_KEY=your_pinecone_api_key
```

## Usage
Start the application with:
```
python pinecone_utility.py
```

In the GUI:
- Use the **Browse** button to select your CSV file.
- Choose the appropriate columns for embedding and metadata.
- Process the CSV to generate and save embeddings. This will create a file called **output.json** in your root folder.

![image](https://github.com/KernAlan/pineconegui/assets/63753020/0bb6deec-d3d8-422b-9d5d-9fefa7c14819)

- Initiate Pinecone with your specific environment details.
- Perform actions like uploading embeddings to Pinecone, fetching data, or deleting vectors.

## Contributing
Contributions to Pinecone Utility GUI are welcome! Feel free to fork the repository, make changes, and submit a pull request.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact
For any questions or support, please reach out to [your contact information or GitHub profile].
