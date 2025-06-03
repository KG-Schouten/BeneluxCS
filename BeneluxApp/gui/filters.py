import tkinter as tk                      # Import Tkinter core
from tkinter import ttk, messagebox       # Import themed widgets (not used here but common)

import re                                  # Import regex for input sanitization

from BeneluxApp.config import FILTERMAXWIDTH

def blend_color(hex_color, alpha):
    """
    Blend a hex color with white, based on alpha (opacity).
    """
    hex_color = hex_color.lstrip('#')    # Remove leading '#' if present
    r = int(hex_color[0:2], 16)           # Parse red component from hex
    g = int(hex_color[2:4], 16)           # Parse green component from hex
    b = int(hex_color[4:6], 16)           # Parse blue component from hex
    # Blend each color channel with white (255) using alpha as weight
    r = int(r * alpha + 255 * (1 - alpha))
    g = int(g * alpha + 255 * (1 - alpha))
    b = int(b * alpha + 255 * (1 - alpha))
    # Return new blended color as hex string
    return f'#{r:02x}{g:02x}{b:02x}'
    
    
class ModernExpandableFrame(tk.Frame):
    """
    A frame with an expandable/collapsible content area with animation.
    """
    def __init__(self, master, animation_duration=200, **kwargs):
        bg = kwargs.pop('bg', None)         # Extract 'bg' from kwargs (default None)
        fg = kwargs.pop('fg', None)         # Extract 'fg' (foreground/text color)
        tx = kwargs.pop('text', '')         # Extract 'text' to show on header label
        ft = kwargs.pop('font', '10')       # Extract 'font' for header, default size 10
        
        super().__init__(master, bg=bg, **kwargs)  # Initialize base Frame with bg

        self.animation_duration = animation_duration  # Total animation duration in ms
        self.animation_steps = 10                      # Number of animation steps (smoothness)
        self.bg = bg                                   # Store background color
        self.fg = fg                                   # Store foreground color
        self.expanded = False                          # Start as collapsed
        
        # Create header Frame with border and background
        self.header = tk.Frame(self, relief=tk.SOLID, bd=1, bg=bg)
        self.header.pack(fill=tk.X)                     # Fill horizontally
        
        # Create arrow icon Label, initially pointing left ('◂')
        self.icon = tk.Label(self.header, text='◂', font=ft, bg=bg, fg=fg)
        self.icon.grid(row=0, column=1, padx=(5,2), sticky='e')  # Right side of header
        
        # Create main header Label with text
        self.label = tk.Label(self.header, text=tx, font=ft, bg=bg, fg=fg)
        self.label.grid(row=0, column=0, sticky='w', padx=2, pady=4)  # Left side
        self.header.grid_columnconfigure(0, weight=1)
        
        # Bind clicks and hover to label and icon
        self.label.bind("<Enter>", lambda e: self._update_label_style(hovered=True))
        self.label.bind("<Leave>", lambda e: self._update_label_style(hovered=False))
        self.label.bind("<Button-1>", lambda e: self.toggle())
        self.icon.bind("<Button-1>", lambda e: self.toggle())
        
        # Bind clicks on header
        self.header.bind("<Button-1>", lambda e: self.toggle())  # Click on header toggles
        self.header.bind("<Enter>", lambda e: self._update_label_style(hovered=True))
        self.header.bind("<Leave>", lambda e: self._update_label_style(hovered=False))
        
        # Clipper Frame controls visible height of extension (content)
        self.clipper = tk.Frame(self, height=0)        # Start with height 0 (collapsed)
        self.clipper.pack(fill=tk.X, expand=False)     # Pack horizontally, no expansion vertically
        self.clipper.pack_forget()                      # Remove clipper from layout if collapsing
        self.clipper.pack_propagate(False)              # Disable auto resize by children
        
        # Extension Frame holds actual expandable content inside clipper
        self.extension = tk.Frame(self.clipper, bg=bg)
        self.extension.pack(fill=tk.BOTH, expand=False)  # Fill inside clipper
        
        # Initialize clipper with height=0 and fully transparent (alpha=0)
        self.clipper.config(height=0)
        self._set_opacity(0.0)
    
    def _update_label_style(self, hovered=False):
        """ Update label style based on hover state."""
        base_bg = self.bg or self.header.cget('bg') or '#f0f0f0'
        
        # For dark mode, blend towards black (instead of white)
        # Let's adjust blend_color to blend towards black if dark mode

        # If base_bg is dark, darken by blending with black (0,0,0)
        # Modify blend_color to optionally blend with any color:
        
        def blend_with_color(hex_color, alpha, blend_to="#000000"):
            hex_color = hex_color.lstrip('#')
            blend_to = blend_to.lstrip('#')
            r1, g1, b1 = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
            r2, g2, b2 = int(blend_to[0:2], 16), int(blend_to[2:4], 16), int(blend_to[4:6], 16)
            r = int(r1 * alpha + r2 * (1 - alpha))
            g = int(g1 * alpha + g2 * (1 - alpha))
            b = int(b1 * alpha + b2 * (1 - alpha))
            return f'#{r:02x}{g:02x}{b:02x}'
        
        dark_bg = blend_with_color(base_bg, 0.7, blend_to="#000000")  # Blend 70% original + 30% black for hover effect
        
        if self.expanded or hovered:
            self.header.config(bg=dark_bg)
            self.label.config(bg=dark_bg, padx=10, fg=self.fg)
            self.icon.config(bg=dark_bg, fg=self.fg)
        else:
            self.header.config(bg=base_bg)
            self.label.config(bg=base_bg, padx=2, fg=self.fg)
            self.icon.config(bg=base_bg, fg=self.fg)
    
    def toggle(self):
        if self.expanded:
            self.collapse()     # If expanded, collapse it
        else:
            self.expand()       # Otherwise, expand it

    def _animate_step(self, height, step, target):
        # (Unused helper method for stepwise animation)
        if abs(height - target) <= abs(step):
            self.extension.place_configure(height=int(target))  # Snap to target height
            self.extension.update()
            return
        self.extension.place_configure(height=int(height))      # Set intermediate height
        self.extension.update()
        self.after(self.animation_duration // self.animation_steps,
                   lambda: self._animate_step(height + step, step, target))

    def _get_target_height(self):
        self.extension.update_idletasks()          # Ensure layout is updated
        return self.extension.winfo_reqheight()    # Get full required height of content
    
    def collapse(self):
        self.icon.config(text='◂')                  # Change arrow to collapsed symbol
        self.expanded = False                        # Mark collapsed state
        current_height = self.clipper.winfo_height()  # Get current height for animation start
        # Animate clipper height down to 0 and opacity to 0, then hide clipper frame
        self.animate(height_from=current_height, height_to=0, alpha_from=1.0, alpha_to=.9, hide_after=True)
        self._update_label_style()

    def expand(self):
        self.icon.config(text='▾')                   # Change arrow to expanded symbol
        self.expanded = True                         # Mark expanded state
        self.clipper.pack(fill=tk.X, expand=False)  # Make sure clipper frame is visible
        self.clipper.update_idletasks()              # Update layout before animation
        target_height = self._get_target_height()    # Calculate content's full height
        # Animate clipper height up to full content height and opacity to 1
        self.animate(height_from=0, height_to=target_height, alpha_from=.9, alpha_to=1.0, hide_after=False)
        self._update_label_style()

    def animate(self, height_from, height_to, alpha_from, alpha_to, hide_after=False):
        # Calculate per-step height and opacity increments for smooth animation
        step_h = (height_to - height_from) / self.animation_steps
        step_a = (alpha_to - alpha_from) / self.animation_steps
        
        def step(i=0):
            new_height = height_from + step_h * i   # Current step height
            new_alpha = alpha_from + step_a * i     # Current step opacity
            
            self.clipper.config(height=int(new_height))  # Set clipper height
            self._set_opacity(new_alpha)                  # Set blended background color
            
            if i < self.animation_steps:
                # Schedule next animation step after delay
                self.after(self.animation_duration // self.animation_steps, lambda: step(i + 1))
            else:
                # Final step: ensure exact target values and hide if needed
                self.clipper.config(height=int(height_to))
                self._set_opacity(alpha_to)
                if hide_after:
                    self.clipper.pack_forget()    # Remove clipper from layout if collapsing
                self.clipper.update()

        step()  # Start the animation
    
    def _set_opacity(self, alpha):
        fade_bg = blend_color(self.bg, alpha)          # Calculate blended background
        self.clipper.config(bg=fade_bg)                 # Apply to clipper frame
        self.extension.config(bg=fade_bg)               # Apply to extension content
        for child in self.extension.winfo_children():   # For each child widget inside extension
            if isinstance(child, (tk.Label, tk.Checkbutton, tk.Button)):  # Widgets supporting bg
                child.config(bg=fade_bg)                 # Set their background too

    
class FilterPanel(tk.Frame):
    def __init__(self, parent, filters_config, callback, bg="#f0f0f0", fg="#000000"):
        super().__init__(parent)
        self.config(width=FILTERMAXWIDTH)
        self.pack_propagate(False) 
        self.callback = callback                  # Function to call when a checkbox changes
        self.filters_config = filters_config      # List of filter groups config
        self.filter_vars = {}                     # Store BooleanVars for all checkboxes
        
        self.bg = bg                            # Background color for the panel
        self.fg = fg                            # Foreground color for text
        
        self.debounce_id = None                # ID for debouncing callback calls
        
        self.build_ui()                           # Build UI elements
    
    def build_ui(self):
        # Title label for the whole filter panel
        tk.Label(self, text="Filters", font=("Arial", 12, "bold"), bg=self.bg, fg=self.fg).pack(anchor="w", pady=5)
        
        # Iterate through each filter group configuration
        for f in self.filters_config:
            col = f["column"]  # Key/name of filter group
            label = f["label"]  # Display label for the group
            ftype = f.get("type", "checkbox")  # Type of filter (default to checkbox)
            options = f["options"]  # List of options for this filter group
            
            # Create an expandable frame with the filter group label
            exp = ModernExpandableFrame(self, bg=self.bg, fg=self.fg, text=label, font=("Arial", 10, "bold"))
            exp.pack(fill=tk.X, pady=2, padx=5)  # Pack with some spacing
            
            if ftype == "input":
                # If filter type is input, create an Entry widget
                var = tk.StringVar()  # Create a StringVar to hold input text
                entry = tk.Entry(exp.extension, textvariable=var, bg=self.bg, fg=self.fg, insertbackground=self.fg)
                entry.pack(fill=tk.X, padx=5, pady=2)  # Pack Entry widget
                self.filter_vars[col] = var  # Store variable to track input text
                
                #  Bind Enter key on the entry widget to call the callback function
                entry.bind("<Return>", lambda e: self.callback())
            
            elif ftype == "radio":
                var = tk.StringVar(value=options[0] if options else "")  # Create StringVar for radio buttons
                self.filter_vars[col] = var
                # Create radio buttons for each option in this group
                for val in options:
                    rb = tk.Radiobutton(exp.extension, text=str(val), variable=var, value=val,
                                        command=self.callback, bg=self.bg, fg=self.fg,
                                        selectcolor=self.bg, anchor="w", justify="left")
                    rb.pack(anchor="w", padx=0)
            
            elif ftype == "slider":
                var = tk.IntVar(value=f.get("min", 0))
                self.filter_vars[col] = var

                # Create label to display current slider value dynamically
                value_label = tk.Label(exp.extension, text=str(var.get()), bg=self.bg, fg=self.fg)
                value_label.pack(anchor="w", padx=5)

                def on_slider_change(val):
                    var.set(val)
                    value_label.config(text=val)
                    # Cancel previously scheduled callback if any
                    if self.debounce_id is not None:
                        self.after_cancel(self.debounce_id)
                    # Schedule callback after 200 ms of inactivity
                    self.debounce_id = self.after(100, self.callback)
                    
                slider = tk.Scale(exp.extension, from_=f.get("min", 0), to=f.get("max", 100),
                                  orient=tk.HORIZONTAL, variable=var, command=on_slider_change,
                                  length=150, bg=self.bg, fg=self.fg, highlightthickness=0)
                slider.pack(anchor="w", padx=5, pady=2)
                            
            else:
                # For checkbox type, create a checkbox for each option
                self.filter_vars[col] = {}  # Initialize dict for this filter group
                for val in options:
                    var = tk.BooleanVar()
                    cb = tk.Checkbutton(exp.extension, text=str(val), variable=var,
                                        command=self.callback, bg=self.bg, fg=self.fg,
                                        selectcolor=self.bg, anchor="w", justify="left")
                    cb.pack(anchor="w", padx=0)     # Pack checkbox aligned left, no padding
                    self.filter_vars[col][val] = var  # Store variable to track checkbox state
                    
    def get_selected_filters(self):
        selected = {}
        for col, var in self.filter_vars.items():
            if isinstance(var, dict):  # multi-select checkboxes
                selected[col] = [val for val, v in var.items() if v.get()]
            elif isinstance(var, (tk.StringVar, tk.IntVar)):  # input, radio, or slider
                value = var.get()
                if isinstance(value, str):
                    value = value.strip()
                    if value:
                        if col in {"event_name", "team_name", "player_name"}:
                            if len(value) < 2:
                                messagebox.showwarning("Invalid Input", "Team name must be at least 3 characters.")
                                print(f"Filter '{col}' must be at least 2 characters.")
                                continue
                        value = re.sub(r'[^\w\s-]', '', value)
                selected[col] = value
        return selected
