# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from BeneluxApp.gui.main_window import PlayersStatsApp

def main():
    app = PlayersStatsApp()
    app.run()
    
if __name__ == "__main__":
    main()