import { syncScrollbars } from "./stats.js";

export class SmartTable {
    constructor(containerSelector, options = {}) {
        this.container = document.querySelector(containerSelector);
        if (!this.container) throw new Error(`Container ${containerSelector} not found`);

        // Look for form in the document, not just within container
        this.form = document.querySelector('#stats-table-form');
        this.tbody = this.container.querySelector('#table-body');
        this.paginationContainer = document.getElementById('pagination');
        this.perPageSelect = this.form?.querySelector('[name="per_page"]');
        this.searchInput = this.form?.querySelector('[name="search"]');

        this.filters = {
            search: '',
            page: 1,
            per_page: parseInt(this.perPageSelect?.value || 25),
            sort: null,
            columns: [], // dynamically managed
            scrollPosition: 0, // Add scroll position tracking
            ...options.initialFilters,
        };        this.stateKey = options.stateKey || 'smartTableState';
        this.dataUrl = options.dataUrl || window.location.pathname;
        this.isRestoringScroll = false; // Flag to prevent scroll event feedback loops

        this.init();
    }

    init() {
        console.log("SmartTable init - form:", this.form);
        console.log("SmartTable init - searchInput:", this.searchInput);
        console.log("SmartTable init - perPageSelect:", this.perPageSelect);
        
        this.loadState();
        this.bindEvents();
        this.bindSortEvents();
        this.fetchData();
    }

    bindEvents() {
        if (this.searchInput) {
            console.log("Binding search input events");
            let timer;
            this.searchInput.addEventListener('input', () => {
                console.log("Search input changed:", this.searchInput.value);
                clearTimeout(timer);
                timer = setTimeout(() => {
                    this.filters.search = this.searchInput.value.trim();
                    this.filters.page = 1;
                    this.filters.scrollPosition = 0; // Reset scroll on search
                    this.fetchData();
                }, 300);
            });
        } else {
            console.log("No search input found");
        }

        if (this.perPageSelect) {
            console.log("Binding per page select events");
            this.perPageSelect.addEventListener('change', () => {
                console.log("Per page changed:", this.perPageSelect.value);
                this.filters.per_page = parseInt(this.perPageSelect.value);
                this.filters.page = 1;
                this.filters.scrollPosition = 0; // Reset scroll on per page change
                this.fetchData();
            });
        } else {
            console.log("No per page select found");
        }
    }

    bindSortEvents() {
        console.log("Binding sort events");
        this.container.querySelectorAll('.sortable').forEach(header => {
            header.addEventListener('click', () => {
                const field = header.dataset.field;
                console.log("Sort clicked on field:", field);
                if (!field) return;

                if (this.filters.sort === field) {
                    this.filters.sort = `-${field}`;
                } else if (this.filters.sort === `-${field}`) {
                    this.filters.sort = null;
                } else {
                    this.filters.sort = field;
                }
                this.filters.page = 1;
                // Don't reset scroll position on sort - keep current position
                console.log("New sort:", this.filters.sort);
                this.fetchData();
            });
        });
    }

    buildQuery() {
        const params = new URLSearchParams();

        if (this.filters.search) params.set('search', this.filters.search);
        if (this.filters.sort) params.set('sort', this.filters.sort);
        if (this.filters.per_page) params.set('per_page', this.filters.per_page);
        if (this.filters.page) params.set('page', this.filters.page);

        // columns omitted for now or can add as needed

        return params.toString();
    }

    saveScrollPosition() {
        const scrollableTable = document.getElementById('scrollable-table');
        const scrollbarTop = document.getElementById('scrollbar-top');
        
        if (scrollableTable) {
            this.filters.scrollPosition = scrollableTable.scrollLeft;
            console.log("Saved scroll position:", this.filters.scrollPosition);
        } else if (scrollbarTop) {
            this.filters.scrollPosition = scrollbarTop.scrollLeft;
            console.log("Saved scroll position (from top bar):", this.filters.scrollPosition);
        }
    }

    restoreScrollPosition() {
        const scrollableTable = document.getElementById('scrollable-table');
        const scrollbarTop = document.getElementById('scrollbar-top');
        
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
        console.log("Fetching URL:", url);

        const container = this.container.querySelector('#stats-table-box');
        console.log("Container found:", container);

        try {
            const res = await fetch(url, {
                headers: { 'X-Requested-With': 'XMLHttpRequest' },
            });
            console.log("Response status:", res.status);
            if (!res.ok) throw new Error(`Status ${res.status}`);

            const html = await res.text();
            console.log("Response HTML length:", html.length);

            if (container) {
                container.innerHTML = html;

                // Rebind events after DOM replaced
                this.bindSortEvents();
                this.bindPaginationEvents();

                // Import and call syncScrollbars, then restore position
                const { syncScrollbars } = await import('./stats.js');
                syncScrollbars();
                
                // Restore scroll position after everything is set up
                setTimeout(() => {
                    this.restoreScrollPosition();
                }, 10); // Minimal delay to ensure syncScrollbars is complete
            }
        } catch (err) {
            console.error('Fetch error:', err);
            if (container) {
                container.innerHTML = `<div class="text-danger">Error: ${err.message}</div>`;
            }
        }
    }

    bindPaginationEvents() {
        if (!this.paginationContainer) return;

        this.paginationContainer.querySelectorAll('.pagination-link').forEach(link => {
            link.addEventListener('click', e => {
                e.preventDefault();
                const page = parseInt(link.dataset.page);
                if (!isNaN(page)) {
                    this.filters.page = page;
                    this.fetchData();
                }
            });
        });
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
                
                // Restore scroll position after a minimal delay to ensure DOM is ready
                setTimeout(() => {
                    this.restoreScrollPosition();
                }, 50);
            } catch (e) {
                console.warn('Invalid saved state');
            }
        }
    }
}