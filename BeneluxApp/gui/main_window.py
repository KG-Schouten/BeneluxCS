# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import tkinter as tk
from tkinter import ttk
from BeneluxApp.gui.filters import FilterPanel
from BeneluxApp.gui.treeview import PlayersTreeview
from database.db_manage import start_database, close_database
from BeneluxApp.config import FILTERS, DARK_ACCENT, DARK_BG, DARK_FG, DARK_HOVER_BG

class PlayersStatsApp:
    def __init__(self):
        self.dark_mode = True  # Set dark mode as default
        self.root = tk.Tk()
        self.root.title("Benelux Players Stats")
        self.root.geometry("1200x800")
        
        if self.dark_mode:
            bg = DARK_BG
            fg = DARK_FG
        else:
            bg = "#f0f0f0"
            fg = "#000000"
            
        self.root.configure(bg=bg)
        
        # Configure grid for responsiveness
        self.root.grid_rowconfigure(0, weight=1)
        self.root.grid_columnconfigure(0, minsize=150) # Ensure column 0 is at least 150 pixels wide
        self.root.grid_columnconfigure(1, weight=1) # Column 1 expands to fill remaining space
        
        # Initialize Filters panel
        self.filters_panel = FilterPanel(self.root, FILTERS, self.on_filter_change, bg=bg, fg=fg)
        self.filters_panel.grid(row=0, column=0, sticky="ns", padx=10, pady=10)
        self.filters_panel.config(bg=bg)
        
        # Initialize Treeview panel
        self.treeview = PlayersTreeview(self.root, bg=bg, fg=fg, dark_mode=self.dark_mode)
        self.treeview.grid(row=0, column=1, sticky="nsew", padx=10, pady=10)

        # Initial data load
        self.apply_filters()
    
    def on_filter_change(self):
        self.apply_filters()
        
    def apply_filters(self):
        filters = self.filters_panel.get_selected_filters()
        
        # Call Database method here to fethch filtered data
        from BeneluxApp.db.db_functions import fetch_filtered_data
        df = fetch_filtered_data(filters)
        self.treeview.populate(df)
    
    def run(self):
        self.root.mainloop()
    