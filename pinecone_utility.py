import os
import threading
from tkinter import ttk
from services.pinecone_service import PineconeService
import logging
import tkinter as tk
from tkinter import filedialog
from tkinter import simpledialog
import queue
from services.csv_service import CSVService
import json
from dotenv import load_dotenv


class PineconeUtility:
    """
    A utility class for interacting with Pinecone API and performing operations on CSV files.

    Args:
        master: The master tkinter window.

    Attributes:
        PINECONE_API_KEY (str): The API key for accessing the Pinecone service.
        OPENAI_API_KEY (str): The API key for accessing the OpenAI service.
        csv_service (CSVService): An instance of the CSVService class for CSV file operations.
        pinecone_service (PineconeService): An instance of the PineconeService class for Pinecone operations.
        log_queue (queue.Queue): A queue for storing log messages.
        logger (logging.Logger): A logger instance for logging messages.
        csv_frame (tk.Frame): The frame for CSV section in the GUI.
        pinecone_frame (tk.Frame): The frame for Pinecone section in the GUI.
        logger_frame (tk.Frame): The frame for logger section in the GUI.
        main_content_column_var (tk.StringVar): The variable for storing the selected main content column.
        main_content_column_dropdown (ttk.Combobox): The dropdown for selecting the main content column.
        metadata_column_var (tk.StringVar): The variable for storing the selected metadata column.
        metadata_column_dropdown (ttk.Combobox): The dropdown for selecting the metadata column.
        csv_file_path (tk.StringVar): The variable for storing the path of the CSV file.
        csv_file_entry (tk.Entry): The entry field for displaying the CSV file path.
        browse_button (tk.Button): The button for browsing and selecting a CSV file.
        process_button (tk.Button): The button for initiating the CSV file processing.
        environment_var (tk.StringVar): The variable for storing the Pinecone environment.
        environment_entry (tk.Entry): The entry field for displaying the Pinecone environment.
        namespace_var (tk.StringVar): The variable for storing the Pinecone namespace.
        namespace_entry (tk.Entry): The entry field for displaying the Pinecone namespace.
        index_name_var (tk.StringVar): The variable for storing the Pinecone index name.
        index_name_entry (tk.Entry): The entry field for displaying the Pinecone index name.
        init_pinecone_button (tk.Button): The button for initializing the Pinecone service.
        json_file_path (tk.StringVar): The variable for storing the path of the JSON file.
        json_file_entry (tk.Entry): The entry field for displaying the JSON file path.
        browse_json_button (tk.Button): The button for browsing and selecting a JSON file.
        upload_button (tk.Button): The button for uploading data to Pinecone.
        fetch_all_button (tk.Button): The button for fetching all data from Pinecone.
        delete_button (tk.Button): The button for deleting data from Pinecone.
        log_text (tk.Text): The text widget for displaying log messages.
    """

    def __init__(self, master):
        ...
class PineconeUtility:
    def __init__(self, master):
        load_dotenv(override=True)
        self.master = master
        self.setup_window()
        self.initialize_logger()
        self.create_frames()
        self.create_csv_section()
        self.create_pinecone_section()
        self.create_logger_section()
        self.initialize_services()
        self.master.after(100, self.check_log_queue)
        self.PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
        self.OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

    def initialize_services(self):
        self.csv_service = CSVService(logger=self.logger)
        self.pinecone_service = PineconeService(logger=self.logger)

    def setup_window(self):
        self.master.title("Pinecone Utility GUI")
        self.master.geometry("850x600")
        self.master.config(bg="#004A8C")
        self.master.grid_columnconfigure(0, weight=0)
        self.master.grid_columnconfigure(1, weight=50)
        self.master.grid_rowconfigure(2, weight=1)

    def initialize_logger(self):
        self.log_queue = queue.Queue()
        self.logger = logging.getLogger()
        handler = TextHandler(self.log_queue)
        handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def create_frames(self):
        self.csv_frame = tk.Frame(self.master, bg="#004A8C")
        self.pinecone_frame = tk.Frame(self.master, bg="#004A8C")
        self.logger_frame = tk.Frame(self.master, bg="#004A8C")
        self.csv_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=10)
        self.pinecone_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=10)
        self.logger_frame.grid(
            row=0, column=1, rowspan=2, sticky="nsew", padx=10, pady=10
        )

    def process_csv_file(self):
        process_thread = threading.Thread(target=self.process_csv_file_thread)
        process_thread.start()

    def process_csv_file_thread(self):
        try:
            CSV_FILE = self.csv_file_path.get()
            df = self.csv_service.read_csv_file(CSV_FILE)

            main_column = self.main_content_column_var.get()
            metadata_column = self.metadata_column_var.get()

            # Process the CSV dataframe
            df = self.csv_service.process_csv_dataframe(
                df, main_column, metadata_column
            )

            vectors = self.csv_service.create_vectors(df, main_column, metadata_column)

            json_output = {"vectors": vectors}
            self.csv_service.save_json_to_file(json_output, "output.json")

            self.log_queue.put("Processing completed successfully")
        except Exception as e:
            self.log_queue.put(f"An error occurred: {str(e)}")

    def create_label(self, frame, text, row, column, pady=5, sticky="w"):
        label = tk.Label(
            frame,
            text=text,
            pady=pady,
            bg="#004A8C",
            fg="#FFFFFF",
            font=("Helvetica", 12, "bold"),
        )
        label.grid(row=row, column=column, sticky=sticky)
        return label

    def create_dropdown(self, frame, variable, row, column, pady=5):
        dropdown = ttk.Combobox(
            frame,
            textvariable=variable,
            width=20,
            font=("Helvetica", 10),
        )
        dropdown.grid(row=row, column=column, pady=pady, sticky="ew")
        return dropdown

    def create_entry(self, frame, variable, row, column, pady=5):
        entry = tk.Entry(
            frame,
            textvariable=variable,
            bg="#FFFFFF",
            fg="#004A8C",
            font=("Helvetica", 10),
        )
        entry.grid(row=row, column=column, pady=pady, sticky="ew")
        return entry

    def create_button(self, frame, text, command, row, column, pady=5, columnspan=1):
        button = tk.Button(
            frame,
            text=text,
            command=command,
            font=("Helvetica", 10),
            bg="#FFFFFF",
            fg="#004A8C",
        )
        button.grid(
            row=row, column=column, pady=pady, columnspan=columnspan, sticky="ew"
        )
        return button

    def create_csv_section(self):
        self.create_label(self.csv_frame, "Column to Embed:", 0, 0)
        self.main_content_column_var = tk.StringVar(self.master)
        self.main_content_column_dropdown = self.create_dropdown(
            self.csv_frame, self.main_content_column_var, 0, 1
        )

        self.create_label(self.csv_frame, "Metadata Column:", 1, 0)
        self.metadata_column_var = tk.StringVar(self.master)
        self.metadata_column_dropdown = self.create_dropdown(
            self.csv_frame, self.metadata_column_var, 1, 1
        )

        self.csv_file_path = tk.StringVar()
        self.csv_file_entry = self.create_entry(
            self.csv_frame, self.csv_file_path, 2, 0
        )
        self.browse_button = self.create_button(
            self.csv_frame, "Browse", self.browse_file, 2, 1
        )
        self.process_button = self.create_button(
            self.csv_frame, "Process CSV", self.process_csv_file, 3, 0, columnspan=2
        )

    def create_pinecone_section(self):
        self.create_label(self.pinecone_frame, "Pinecone Environment:", 0, 0)
        self.environment_var = tk.StringVar(self.master)
        self.environment_entry = self.create_entry(
            self.pinecone_frame, self.environment_var, 0, 1
        )

        self.create_label(self.pinecone_frame, "Pinecone Namespace:", 1, 0)
        self.namespace_var = tk.StringVar(self.master)
        self.namespace_entry = self.create_entry(
            self.pinecone_frame, self.namespace_var, 1, 1
        )

        self.create_label(self.pinecone_frame, "Pinecone Index Name:", 2, 0)
        self.index_name_var = tk.StringVar(self.master)
        self.index_name_entry = self.create_entry(
            self.pinecone_frame, self.index_name_var, 2, 1
        )

        self.init_pinecone_button = self.create_button(
            self.pinecone_frame,
            "Initialize Pinecone",
            self.init_pinecone,
            3,
            0,
            columnspan=2,
        )

        # Create an entry field for JSON file path
        self.json_file_path = tk.StringVar()
        self.json_file_entry = self.create_entry(
            self.pinecone_frame, self.json_file_path, 4, 0
        )

        # Create a "Browse JSON" button
        self.browse_json_button = self.create_button(
            self.pinecone_frame, "Browse JSON", self.browse_json_file, 4, 1
        )

        # Button for uploading to Pinecone
        self.upload_button = self.create_button(
            self.pinecone_frame,
            "Upload to Pinecone",
            self.upload_to_pinecone,
            5,
            0,
            columnspan=2,
        )

        # Button for fetching all from Pinecone
        self.fetch_all_button = self.create_button(
            self.pinecone_frame,
            "Fetch All from Pinecone",
            self.fetch_all_from_pinecone,
            6,
            0,
            columnspan=2,
        )

        # Button for deleting from Pinecone
        self.delete_button = self.create_button(
            self.pinecone_frame,
            "Delete from Pinecone",
            self.delete_from_pinecone,
            7,
            0,
            columnspan=2,
        )

    def init_pinecone(self):
        try:
            environment = self.environment_var.get()
            namespace = self.namespace_var.get()
            index_name = self.index_name_var.get()
            pinecone_api_key = self.PINECONE_API_KEY
            # Assuming init_pinecone is a method of PineconeService that initializes the service
            self.pinecone_service.init_pinecone(
                environment, index_name, pinecone_api_key, namespace
            )
            self.log_queue.put("Pinecone initialized successfully.")
        except Exception as e:
            self.log_queue.put(
                f"An error occurred during Pinecone initialization: {str(e)}"
            )

    # Method for uploading to Pinecone
    def upload_to_pinecone(self):
        upload_thread = threading.Thread(target=self.upload_to_pinecone_thread)
        upload_thread.start()

    # Method for fetching all from Pinecone
    def fetch_all_from_pinecone(self):
        fetch_all_thread = threading.Thread(target=self.fetch_all_from_pinecone_thread)
        fetch_all_thread.start()

    def fetch_all_from_pinecone_thread(self):
        try:
            self.log_queue.put("Starting Pinecone fetch all...")
            all_data = self.pinecone_service.fetch_all_vectors_and_metadata()
            self.log_queue.put(
                f"Fetched all data from Pinecone. Total records: {len(all_data)}"
            )
        except Exception as e:
            self.log_queue.put(f"An error occurred during Pinecone fetch all: {str(e)}")

    # Method for deleting from Pinecone
    def delete_from_pinecone(self):
        delete_thread = threading.Thread(target=self.delete_from_pinecone_thread)
        delete_thread.start()

    def delete_from_pinecone_thread(self):
        try:
            delete_id = simpledialog.askstring(
                "Delete from Pinecone", "Enter the ID to delete:"
            )
            if delete_id:
                self.pinecone_service.delete_vectors(ids=[delete_id])
                self.log_queue.put(
                    f"Vector {delete_id} successfully deleted from Pinecone."
                )
        except Exception as e:
            self.log_queue.put(f"An error occurred during Pinecone delete: {str(e)}")

    def create_logger_section(self):
        # Put a label for the logger section
        self.create_label(self.logger_frame, "Output Logs", 0, 0)
        self.log_text = tk.Text(
            self.logger_frame,
            bg="#FFFFFF",
            fg="#004A8C",
            font=("Helvetica", 10),
            height=20,
        )
        self.log_text.grid(row=1, column=0, sticky="ew", padx=10, pady=10, rowspan=5)
        log_scrollbar = tk.Scrollbar(self.logger_frame, command=self.log_text.yview)
        log_scrollbar.grid(row=1, column=1, sticky="nsew")
        self.log_text["yscrollcommand"] = log_scrollbar.set

    def browse_file(self):
        file_path = filedialog.askopenfilename(
            filetypes=(("CSV files", "*.csv"), ("All files", "*.*"))
        )
        if file_path:
            self.csv_file_path.set(file_path)
            self.read_csv(file_path)

    def read_csv(self, file_path):
        try:
            self.csv_df = self.csv_service.read_csv_file(file_path)
            columns = self.csv_df.columns.tolist()

            # Update dropdowns for Column to Embed and Metadata Column
            self.main_content_column_dropdown["values"] = columns
            self.metadata_column_dropdown["values"] = columns

            # Set default values
            if columns:
                self.main_content_column_var.set(columns[0])
                self.metadata_column_var.set(columns[0] if len(columns) > 1 else "")
        except Exception as e:
            self.log_queue.put(
                f"An error occurred while reading the CSV file: {str(e)}"
            )

    def process_csv_file(self):
        process_thread = threading.Thread(target=self.process_csv_file_thread)
        process_thread.start()

    def process_csv_file_thread(self):
        try:
            CSV_FILE = self.csv_file_path.get()
            if not CSV_FILE:
                self.log_queue.put("No CSV file selected for processing.")
                return
            df = self.csv_service.read_csv_file(CSV_FILE)
            if df is None:
                self.log_queue.put("Error reading CSV file.")
                return
            main_column = self.main_content_column_var.get()
            metadata_column = self.metadata_column_var.get()

            new_df = self.csv_service.process_csv_dataframe(
                df, main_column, metadata_column
            )

            vectors = self.csv_service.create_vectors(
                new_df, main_column, metadata_column
            )

            json_output = {"vectors": vectors}
            output_path = "output.json"
            self.csv_service.save_json_to_file(json_output, output_path)
            self.log_queue.put("Processing completed successfully")
        except Exception as e:
            self.log_queue.put(f"An error occurred during file processing: {str(e)}")

    def upload_to_pinecone_thread(self):
        try:
            json_file_path = self.json_file_path.get()
            if not json_file_path:
                self.log_queue.put("No JSON file selected for upload.")
                return

            with open(json_file_path, "r") as json_file:
                data = json.load(json_file)

            vectors = data.get("vectors", [])
            if not vectors:
                self.log_queue.put("No vectors found in the JSON file.")
                return

            # Assuming upsert_vectors is a method in PineconeService that uploads the vectors
            self.pinecone_service.upsert_vectors(vectors)
            self.log_queue.put(
                f"Successfully uploaded {len(vectors)} vectors to Pinecone."
            )
        except Exception as e:
            self.log_queue.put(f"An error occurred during upload to Pinecone: {str(e)}")

    def fetch_all_from_pinecone(self):
        fetch_all_thread = threading.Thread(target=self.fetch_all_from_pinecone_thread)
        fetch_all_thread.start()

    def fetch_all_from_pinecone_thread(self):
        try:
            # Assuming fetch_all_vectors is a method in PineconeService that fetches all vectors
            all_vectors = self.pinecone_service.fetch_all_vectors()
            self.log_queue.put(f"Fetched {len(all_vectors)} vectors from Pinecone.")
        except Exception as e:
            self.log_queue.put(
                f"An error occurred during fetching from Pinecone: {str(e)}"
            )

    def browse_json_file(self):
        try:
            file_path = filedialog.askopenfilename(
                filetypes=(("JSON files", "*.json"), ("All files", "*.*"))
            )
            self.json_file_path.set(file_path)
        except Exception as e:
            self.log_queue.put(f"Error while browsing JSON file: {str(e)}")

    def delete_from_pinecone(self):
        delete_thread = threading.Thread(target=self.delete_from_pinecone_thread)
        delete_thread.start()

    def delete_from_pinecone_thread(self):
        try:
            delete_id = simpledialog.askstring(
                "Delete from Pinecone", "Enter the ID to delete:"
            )
            if delete_id is None:
                self.log_queue.put("Delete operation cancelled.")
                return
            self.log_queue.put(f"Starting Pinecone delete for ID: {delete_id}...")
            self.pinecone_service.init_pinecone()
            self.pinecone_service.delete_vectors(ids=[delete_id])
            self.log_queue.put(
                f"Vector {delete_id} successfully deleted from Pinecone."
            )
        except Exception as e:
            self.log_queue.put(f"An error occurred during Pinecone delete: {str(e)}")

    def check_log_queue(self):
        while not self.log_queue.empty():
            log_entry = self.log_queue.get()
            self.log_text.insert(tk.END, log_entry + "\n")
            self.log_text.see(tk.END)

        # Run again after 100 milliseconds
        self.master.after(100, self.check_log_queue)


class TextHandler(logging.Handler):
    def __init__(self, log_queue):
        logging.Handler.__init__(self)
        self.log_queue = log_queue

    def emit(self, record):
        log_entry = self.format(record)
        self.log_queue.put(log_entry)


if __name__ == "__main__":
    root = tk.Tk()
    app = PineconeUtility(root)
    root.mainloop()
