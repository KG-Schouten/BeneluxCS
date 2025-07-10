/**
 * Sort Indicators - Unified sort indicator management for SmartTable
 * 
 * This module provides functions to manage sort indicators across different
 * tables in a consistent way, regardless of the actual HTML structure.
 */

/**
 * Apply sorting class to header element and its child indicator
 * @param {HTMLElement} header - The header element to update
 * @param {string} sortClass - The sort class to apply ('sort-asc', 'sort-desc', or '')
 * @param {object} config - Configuration for indicator element and classes
 */
function updateSortIndicator(header, sortClass, config = {}) {
  // Default configuration
  const defaults = {
    indicatorSelector: '.sort-indicator',  // Selector for indicator element
    ascClass: 'fa-sort-up',               // Class for ascending sort
    descClass: 'fa-sort-down',            // Class for descending sort
    neutralClass: 'fa-sort',              // Class for no sort
  };
  
  // Merge provided config with defaults
  const options = { ...defaults, ...config };
  
  // First, update the header element classes
  header.classList.remove('sort-asc', 'sort-desc');
  if (sortClass) {
    header.classList.add(sortClass);
  }
  
  // Then, find and update the indicator element if it exists
  const indicator = header.querySelector(options.indicatorSelector);
  if (indicator) {
    // Remove all possible classes
    indicator.classList.remove(
      options.ascClass, 
      options.descClass, 
      options.neutralClass
    );
    
    // Apply the appropriate class
    if (sortClass === 'sort-asc') {
      indicator.classList.add(options.ascClass);
    } else if (sortClass === 'sort-desc') {
      indicator.classList.add(options.descClass);
    } else {
      indicator.classList.add(options.neutralClass);
    }
  }
}

/**
 * Update all sort indicators in a container based on current sort state
 * @param {HTMLElement} container - The container element with sortable headers
 * @param {string} currentSort - The current sort field (e.g., 'name' or '-name')
 * @param {object} config - Configuration for indicator element and classes
 */
export function updateSortIndicators(container, currentSort, config = {}) {
  if (!container) return;
  
  // Get all sortable headers
  const headers = container.querySelectorAll('.sortable');
  
  // Remove sorting classes from all headers
  headers.forEach(header => {
    updateSortIndicator(header, '', config);
  });
  
  // If we have a current sort, find the header and apply the right class
  if (currentSort) {
    let field = currentSort;
    let sortClass = 'sort-asc';
    
    // Check if it's a descending sort (starts with -)
    if (currentSort.startsWith('-')) {
      field = currentSort.substring(1);
      sortClass = 'sort-desc';
    }
    
    // Find the header with the matching data-field attribute
    const header = container.querySelector(`.sortable[data-field="${field}"]`);
    if (header) {
      updateSortIndicator(header, sortClass, config);
    }
  }
}

/**
 * Enhance SmartTable with unified sort indicator handling
 * @param {SmartTable} table - The SmartTable instance
 * @param {object} config - Configuration for indicator element and classes
 */
export function enhanceTableSorting(table, config = {}) {
  if (!table || !table.container) {
    console.error('Invalid SmartTable instance');
    return;
  }
  
  // Store original bindSortEvents method
  const originalBindSortEvents = table.bindSortEvents;
  
  // Override bindSortEvents method with our enhanced version
  table.bindSortEvents = function() {
    console.log("Enhanced binding sort events");
    
    this.container.querySelectorAll('.sortable').forEach(header => {
      const field = header.dataset.field;

      header.addEventListener('click', () => {
        if (!field) return;

        // Update sorting filter
        if (this.filters.sort === field) {
          this.filters.sort = `-${field}`;
        } else if (this.filters.sort === `-${field}`) {
          this.filters.sort = null;
        } else {
          this.filters.sort = field;
        }

        this.filters.page = 1;

        // Update header classes using our unified approach
        updateSortIndicators(this.container, this.filters.sort, config);
        
        this.fetchData();
      });
    });

    // On load, apply correct sort classes based on current sort filter
    updateSortIndicators(this.container, this.filters.sort, config);
  };
  
  // If the table is already initialized, rebind the sort events
  table.bindSortEvents();
}
