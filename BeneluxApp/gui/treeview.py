import tkinter as tk
from tkinter import ttk
import pandas as pd
import numpy as np

class PlayersTreeview(tk.Frame):
    def __init__(self, parent, bg="#f0f0f0", fg="#000000", dark_mode=False):
        super().__init__(parent)

        self.image_cache = {}  # Prevents Tk from removing the images
        
        self.dark_mode = dark_mode
        self.bg = bg
        self.fg = fg
        
        self.sort_column = None
        self.sort_reverse = False
        
        style = ttk.Style()
        if self.dark_mode:
            style.theme_use('clam')
            style.configure("Treeview",
                            background=self.bg,
                            foreground=self.fg,
                            fieldbackground=self.bg,
                            highlightthickness=0,
                            borderwidth=0,
                            rowheight=25)
            style.configure("Vertical.TScrollbar",
                            background=self.bg,
                            troughcolor=self.bg,
                            bordercolor=self.bg,
                            arrowcolor=self.fg)
            style.configure("Horizontal.TScrollbar",
                            background=self.bg,
                            troughcolor=self.bg,
                            bordercolor=self.bg,
                            arrowcolor=self.fg)
            style.map('Treeview', background=[('selected', '#666666')], foreground=[('selected', '#ffffff')])
        else:
            style.theme_use('default')
        
        # Grid layout for flexibility
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)
        
        # Treeview widget
        self.tree = ttk.Treeview(self, show='tree headings', selectmode='extended')
        self.tree.grid(row=0, column=0, sticky="nsew")
        
        # Set up the 0th column for flags
        self.tree.column("#0", width=10, anchor="center")
        self.tree.heading("#0", text="")  # optional: add header like "Flag"

        
        # Scrollbars
        vsb = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(self, orient="horizontal", command=self.tree.xview)
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

    def populate(self, df):
        # Clear existing data
        self.tree.delete(*self.tree.get_children())

        self.data = df.reset_index(drop=True)  # Save for sorting
        visible_columns = [col for col in df.columns if col not in ['player_id', 'country']]  # Exclude player_id from display
        self.tree["columns"] = visible_columns
        
        # Setup columns (names) dynamically
        for col in visible_columns:
            self.tree.heading(col, text=col, command=lambda c=col: self.sort_by_column(c))
            self.tree.column(col, width=100, anchor="center")

        self.data = df.reset_index(drop=True)  # Save for sorting
        self.insert_rows(df) # Insert the rows into the treeview
    
    def insert_rows(self, df):
        self.tree.delete(*self.tree.get_children())
        
        for idx, row in df.iterrows():
            # Load flag image
            if 'country' in row and pd.notna(row['country']):
                country_code = row['country'].upper()
                flag_path = f"BeneluxApp/gui/Images/Flags/{country_code}.png"  # use .png instead of .svg
                try:
                    flag_img = tk.PhotoImage(file=flag_path)
                    self.image_cache[f"{idx}_{country_code}"] = flag_img
                except Exception as e:
                    print(f"Error loading flag for {country_code}: {e}")
                    flag_img = tk.PhotoImage(file="BeneluxApp/gui/Images/Flags/Unknown.png")  # Fallback if image loading fails
            else:
                flag_img = tk.PhotoImage(file="BeneluxApp/gui/Images/Flags/Unknown.png")
        
            # Extract visible values for the other columns
            values = [row[col] for col in self.tree["columns"]]
            self.tree.insert("", "end", image=flag_img, values=values)
 
    def sort_by_column(self, col):
        if self.sort_column == col:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = col
            self.sort_reverse = False
        
        # Work on a copy of the data
        df = self.data.copy()
        
        try:
            # Try numeric sort
            temp_col = pd.to_numeric(df[col], errors='raise')  # Will raise if any bad data
            sorted_idx = temp_col.argsort(kind='mergesort')
        except Exception:
            try:
                # Try string sort
                temp_col = df[col].astype(str)  # Convert to string, will raise if bad data
                sorted_idx = temp_col.argsort(kind='mergesort')
            except Exception as e:
                # If both numeric and string sort fail, fallback to default sort
                print(f"Cannot sort column '{col}': {e}")
                return # Do nothing if even string sort fails
        
        # Reverse index if needed
        if self.sort_reverse:
            sorted_idx = sorted_idx[::-1]
            
        sorted_df = df.iloc[sorted_idx].reset_index(drop=True)
        self.data = sorted_df
        self.insert_rows(self.data)