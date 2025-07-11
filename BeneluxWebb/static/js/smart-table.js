import { bindPaginationEvents } from './pagination.js';

export class SmartTable {
    constructor(containerSelector, options = {}) {
        this.options = {
            formSelector: '#stats-table-form',           // Default form selector
            tableBoxSelector: '#stats-table-box',        // Default table box selector
            tableBodySelector: '#table-body',            // Default table body selector
            paginationSelector: '#pagination',           // Default pagination selector
            scrollableTableSelector: '#scrollable-table',// Default scrollable table selector
            scrollbarTopSelector: '#scrollbar-top',      // Default scrollbar top selector
            perPageSelector: '[name="per_page"]',        // Default per page selector
            searchSelector: '[name="search"]',           // Default search selector
            ...options                                   // Override with provided options
        };
        
        this.container = document.querySelector(containerSelector);
        if (!this.container) throw new Error(`Container ${containerSelector} not found`);

        // Find the form either within the container or by selector
        this.form = options.form || 
                    (this.options.formSelector.startsWith('#') ? 
                     document.querySelector(this.options.formSelector) : 
                     this.container.querySelector(this.options.formSelector));
        
        // The table-specific container that will be updated with AJAX responses
        this.tableContainer = this.container.querySelector(this.options.tableBoxSelector);
        if (!this.tableContainer) throw new Error(`Table container ${this.options.tableBoxSelector} not found`);
        
        this.tbody = this.container.querySelector(this.options.tableBodySelector);
        this.paginationContainer = document.querySelector(this.options.paginationSelector);
        this.perPageSelect = this.form?.querySelector(this.options.perPageSelector);
        this.searchInput = this.form?.querySelector(this.options.searchSelector);
        
        // Log what was found for debugging
        console.log("SmartTable init - selectors found:", {
            container: this.container,
            form: this.form,
            tableContainer: this.tableContainer,
            tbody: this.tbody,
            pagination: this.paginationContainer,
            perPage: this.perPageSelect,
            search: this.searchInput
        });

        this.filters = {
            page: 1,
            per_page: parseInt(this.perPageSelect?.value || 25),
            scrollPosition: 0,
            ...options.initialFilters,
        };

        this.stateKey = options.stateKey || 'smartTableState';
        this.dataUrl = options.dataUrl || window.location.pathname;
        this.isRestoringScroll = false;
        
        // Store custom filter widgets
        this.filterWidgets = {};

        // Add support for after-render callbacks
        this.afterRenderCallbacks = [];

        this.init();
    }

    init() {
        console.log("SmartTable init - form:", this.form);
        console.log("SmartTable init - searchInput:", this.searchInput);
        console.log("SmartTable init - perPageSelect:", this.perPageSelect);
        
        this.loadState();
        this.bindFilterInputs();
        this.bindResetButton();
        this.bindSortEvents();
        this.fetchData();
    }

    bindFilterInputs() {
        if (!this.form) {
            console.warn('No form found for filter inputs');
            return;
        }

        console.log('Binding filter inputs on form:', this.form);
        const inputs = this.form.querySelectorAll('[name]');

        inputs.forEach(input => {
            const name = input.name;

            const updateFilter = () => {
                let value;
                if (input.type === 'checkbox') {
                    // Generalized handling for multi-checkbox filters
                    const checkboxes = this.form.querySelectorAll(`input[name="${name}"]`);
                    const checkedValues = Array.from(checkboxes)
                        .filter(cb => cb.checked)
                        .map(cb => cb.value);

                    // If there are multiple checkboxes with the same name, treat it as a multi-checkbox filter
                    value = checkboxes.length > 1 ? checkedValues : input.checked;
                } else if (input.tagName === 'SELECT' && input.multiple) {
                    value = Array.from(input.selectedOptions).map(opt => opt.value);
                } else {
                    value = input.value;
                }

                this.filters[name] = value;
                this.filters.page = 1; // Reset page on filter change
                this.filters.scrollPosition = 0; // Reset scroll position
                console.log(`Filter updated: ${name}=${JSON.stringify(value)}`, JSON.stringify(this.filters));
                this.saveState();
                this.fetchData();
            };

            if (input.type === 'text' || input.type === 'search') {
                let timer;
                input.addEventListener('input', () => {
                    clearTimeout(timer);
                    timer = setTimeout(updateFilter, 300);
                    console.log(`Input event on ${name}, timer set`);
                });
            } else {
                input.addEventListener('change', () => {
                    console.log(`Change event on ${name}`);
                    updateFilter();
                });
            }
        });
    }

    bindResetButton() {
        const resetBtn = this.form?.querySelector('#reset-filters-btn');
        if (!resetBtn) return;

        resetBtn.addEventListener('click', () => {
            // Reset form inputs
            this.form.reset();

            // Reinitialize filters from defaults
            this.filters = {
                page: 1,
                per_page: parseInt(this.perPageSelect?.value || 25),
                scrollPosition: 0
            };

            // Reapply initial filter values if needed
            if (this.options?.initialFilters) {
                Object.assign(this.filters, this.options.initialFilters);
            }

            this.saveState();
            this.fetchData();
        });
    }

    bindSortEvents() {
        console.log("Binding sort events (default implementation)");

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

                // Update header classes
                this.container.querySelectorAll('.sortable').forEach(h => {
                    h.classList.remove('sort-asc', 'sort-desc');
                });

                if (this.filters.sort === field) {
                    header.classList.add('sort-asc');
                } else if (this.filters.sort === `-${field}`) {
                    header.classList.add('sort-desc');
                }

                this.fetchData();
            });
        });

        // On load, apply correct sort classes based on current sort filter
        const sortField = this.filters.sort;
        if (sortField) {
            const field = sortField.replace('-', '');
            const header = this.container.querySelector(`.sortable[data-field="${field}"]`);
            if (header) {
                if (sortField.startsWith('-')) {
                    header.classList.add('sort-desc');
                } else {
                    header.classList.add('sort-asc');
                }
            }
        }
        
        // Note: This default implementation can be overridden by sort-indicators.js
        // using the enhanceTableSorting() function
    }

    buildQuery() {
        const params = new URLSearchParams();

        for (const key in this.filters) {
            const value = this.filters[key];

            // Skip internal filter state properties that shouldn't be in the URL
            if (key === 'scrollPosition' || key === 'isRestoringScroll') {
                continue;
            }

            if (Array.isArray(value)) {
                // Convert array to comma-separated string
                const joined = value.join(',');
                params.set(key, joined);
                console.log(`Added array param: ${key}=${joined}`);
            } else if (value !== null && value !== undefined && value !== '') {
                params.set(key, value);
                console.log(`Added param: ${key}=${value}`);
            }
        }

        const queryString = params.toString();
        console.log(`Built query string: ${queryString}`);
        return queryString;
    }

    saveScrollPosition() {
        const scrollableTable = document.querySelector(this.options.scrollableTableSelector);
        const scrollbarTop = document.querySelector(this.options.scrollbarTopSelector);
        
        if (scrollableTable) {
            this.filters.scrollPosition = scrollableTable.scrollLeft;
            console.log("Saved scroll position:", this.filters.scrollPosition);
        } else if (scrollbarTop) {
            this.filters.scrollPosition = scrollbarTop.scrollLeft;
            console.log("Saved scroll position (from top bar):", this.filters.scrollPosition);
        }
    }

    restoreScrollPosition() {
        const scrollableTable = document.querySelector(this.options.scrollableTableSelector);
        const scrollbarTop = document.querySelector(this.options.scrollbarTopSelector);
        
        if (this.filters.scrollPosition && scrollableTable) {
            // Disable scroll event tracking temporarily to prevent feedback loops
            this.isRestoringScroll = true;
            
            // Use requestAnimationFrame for smoother restoration
            requestAnimationFrame(() => {
                scrollableTable.scrollLeft = this.filters.scrollPosition;
                if (scrollbarTop) {
                    scrollbarTop.scrollLeft = this.filters.scrollPosition;
                }
                console.log("Restored scroll position:", this.filters.scrollPosition);
                
                // Re-enable scroll tracking after a brief delay
                setTimeout(() => {
                    this.isRestoringScroll = false;
                }, 50);
            });
        }
    }

    async fetchData() {
        console.log("fetchData called with filters:", this.filters);
        
        // Save scroll position before fetching
        this.saveScrollPosition();
        this.saveState();

        const query = this.buildQuery();
        const url = `${this.dataUrl}?${query}`;
        history.replaceState(null, '', url);
        console.log("Fetching URL:", url);

        try {
            console.log("Sending AJAX request to:", url);
            const res = await fetch(url, {
                headers: { 'X-Requested-With': 'XMLHttpRequest' },
            });
            console.log("Response status:", res.status);
            if (!res.ok) throw new Error(`Status ${res.status}`);

            const html = await res.text();
            console.log("Response HTML length:", html.length);

            // Update ONLY the table container's content with the fetched HTML, not the entire wrapper
            this.tableContainer.innerHTML = html;
            console.log("Table container updated with new HTML");

            // Rebind events after DOM replaced
            this.bindSortEvents();
            
            // Rebind pagination events
            bindPaginationEvents('.pagination-link', (page) => {
                this.filters.page = page;
                this.saveState();
                console.log("Pagination link clicked, new page:", page);
                this.fetchData(); // Call this instance's fetchData method
            });

            // // Import and call syncScrollbars with the proper selectors, then restore position
            // const { syncScrollbars } = await import('./stats.js');
            // syncScrollbars(this.options.scrollableTableSelector, this.options.scrollbarTopSelector);
            
            // Restore scroll position after everything is set up
            setTimeout(() => {
                this.restoreScrollPosition();
            }, 10); // Minimal delay to ensure syncScrollbars is complete
            
            // Trigger afterRender callbacks
            this.afterRenderCallbacks.forEach(cb => {
                try {
                    cb();
                } catch (e) {
                    console.error('afterRender callback error:', e);
                }
            });

        } catch (err) {
            console.error('Fetch error:', err);
            this.tableContainer.innerHTML = `<div class="text-danger">Error: ${err.message}</div>`;
        }
    }

    saveState() {
        localStorage.setItem(this.stateKey, JSON.stringify(this.filters));
    }

    loadState() {
        const stored = localStorage.getItem(this.stateKey);
        if (stored) {
            try {
                this.filters = {
                    ...this.filters,
                    ...JSON.parse(stored),
                };

                // Update form inputs based on loaded state
                if (this.searchInput) this.searchInput.value = this.filters.search || '';
                if (this.perPageSelect) this.perPageSelect.value = this.filters.per_page || 25;
                
                // Update filter widgets values based on current filters
                this.updateFilterWidgetsFromState();
                
                // Restore scroll position after a minimal delay to ensure DOM is ready
                setTimeout(() => {
                    this.restoreScrollPosition();
                }, 50);
            } catch (e) {
                console.warn('Invalid saved state');
            }
        }
    }
    
    /**
     * Registers a filter widget with this SmartTable instance
     * @param {string} name - Name to identify the widget
     * @param {object} widget - Filter widget instance
     */
    registerFilterWidget(name, widget) {
        if (!widget || typeof widget.init !== 'function') {
            console.error(`Invalid widget provided for name "${name}"`);
            return;
        }
        
        this.filterWidgets[name] = widget;
        console.log(`Registered filter widget "${name}"`, widget);
    }
    
    /**
     * Updates all filter widgets from the current state
     */
    updateFilterWidgetsFromState() {
        Object.values(this.filterWidgets).forEach(widget => {
            if (widget && typeof widget.setValue === 'function') {
                // Each widget should know how to extract its value from filters
                try {
                    widget.setValue(widget.getValue());
                } catch (e) {
                    console.warn('Error updating widget from state:', e);
                }
            }
        });
    }

    /**
    * Registers a callback to run after table render completes
    * @param {Function} callback 
    */
    onAfterRender(callback) {
        if (typeof callback === 'function') {
            this.afterRenderCallbacks.push(callback);
        }
    }
}