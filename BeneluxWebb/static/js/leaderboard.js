import { FilterState } from './filter-state.js';
import { bindPaginationEvents } from './pagination.js';

const filterState = new FilterState('leaderboardFilters', {
    form: '#leaderboard-table-form',
    search: '#search',
    perPage: '#per-page-select',
    countries: 'input[name="countries"]',
    eloSlider: '#elo-slider',
});

document.addEventListener('DOMContentLoaded', () => {
    try {
        // DOM element references used throughout the script
        const eloSlider = document.getElementById('elo-slider');
        const form = document.getElementById('leaderboard-table-form');
        const tableContainer = document.querySelector('.leaderboard-table-box');
        const searchInput = document.getElementById('search');
        const perPageSelect = document.getElementById('per-page-select');

        // Early exit if critical elements missing
        if (!form || !eloSlider) {
        alert('Critical error: missing #leaderboard-table-form or #elo-slider');
        return;
        }

        let debounceTimer; // used for debouncing search input
        let sliderDebounceTimer; // used for debouncing slider changes

        /**
         * Build a URL query string from the current filter and sort parameters.
         * Used to request filtered data from the backend.
         */
        function buildQueryParams() {
            const filters = filterState.getFilters();
            const params = new URLSearchParams();

            if (filters.search) params.set('search', filters.search);
                filters.countries.forEach(c => params.append('countries', c));
                params.set('min_elo', filters.min_elo);
                params.set('max_elo', filters.max_elo);
                params.set('per_page', filters.per_page);
            if (filters.sort) params.set('sort', filters.sort);
            if (filters.page) params.set('page', filters.page);

            return params.toString();
        }

        /**
         * Fetch leaderboard HTML from the server using current filters and update the table.
         * Shows loading spinner while waiting and error message if request fails.
         * Rebinds event handlers for sorting and pagination after content update.
         */
        async function fetchLeaderboard() {
            let min = 0, max = 5000;

            if (eloSlider && eloSlider.noUiSlider) {
                [min, max] = eloSlider.noUiSlider.get().map(v => Math.round(v));
            }

            filterState.save(min, max);

            const query = buildQueryParams();
            const url = `/leaderboard?${query}`;

            try {
                if (tableContainer) {
                tableContainer.innerHTML = `
                    <div class="text-center my-5">
                    <div class="spinner-border text-light" role="status"><span class="visually-hidden">Loading...</span></div>
                    <p class="text-light mt-2">Loading data...</p>
                    </div>`;
                }

                const response = await fetch(url, {
                headers: {
                    'X-Requested-With': 'XMLHttpRequest',
                    'Cache-Control': 'no-cache'
                }
                });

                if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);

                const html = await response.text();

                if (tableContainer) {
                    tableContainer.innerHTML = html;
                    bindSortEvents();

                    // Use filterState.setPage() to keep page consistent in module
                    bindPaginationEvents('.pagination-link', (page) => {
                        filterState.setPage(page);
                        fetchLeaderboard();
                    });
                }
            } catch (error) {
                if (tableContainer) {
                tableContainer.innerHTML = `
                    <div class="alert alert-danger my-3">
                    <h4 class="alert-heading">Error loading data!</h4>
                    <p>${error.message}</p>
                    <hr>
                    <button class="btn btn-outline-danger" onclick="testFetch()">Retry</button>
                    </div>`;
                }
            }
        }
    
        /**
         * Attach click event listeners to column headers that support sorting.
         * Clicking a header updates sort order and reloads leaderboard data.
         */
        function bindSortEvents() {
            document.querySelectorAll('.sort-header').forEach(header => {
                header.addEventListener('click', e => {
                e.preventDefault();
                form.dataset.sort = header.dataset.sort;
                form.dataset.page = "1"; // reset page on new sort
                fetchLeaderboard();
                });
            });
        }

        // Event listeners for filters that trigger data reload
        document.querySelectorAll('input[name="countries"]').forEach(cb => {
            cb.addEventListener('change', () => {
                filterState.setPage(1);
                fetchLeaderboard();
            });
        });

        perPageSelect.addEventListener('change', () => {
            filterState.setPage(1);
            fetchLeaderboard();
        });

        // Debounced input event for search to reduce requests while typing
        searchInput.addEventListener('input', () => {
            clearTimeout(debounceTimer);
            debounceTimer = setTimeout(() => {
                filterState.setPage(1);
                fetchLeaderboard();
            }, 150);
        });

        // Check if noUiSlider library loaded and initialize slider only once
        if (typeof noUiSlider === 'undefined') {
            throw new Error('noUiSlider library not loaded');
        }

        if (!eloSlider.noUiSlider) {
            noUiSlider.create(eloSlider, {
                start: [0, 5000],
                connect: true,
                range: { min: 0, max: 5000 },
                step: 50,
                behaviour: 'tap-drag',
                format: {
                to: value => Math.round(value),
                from: value => Number(value)
                }
            });
        }

        /**
         * Update displayed ELO min/max values below the slider.
         */
        function updateEloDisplay() {
            const [min, max] = eloSlider.noUiSlider.get().map(v => Math.round(v));
            document.getElementById('elo-min-value').textContent = min;
            document.getElementById('elo-max-value').textContent = max;
        }

        eloSlider.noUiSlider.on('update', updateEloDisplay);

        eloSlider.noUiSlider.on('change', () => {
            clearTimeout(sliderDebounceTimer);
            sliderDebounceTimer = setTimeout(() => {
                filterState.setPage(1);
                fetchLeaderboard();
            }, 200);
        });

        // Load saved filters from localStorage and update UI
        filterState.load();
        updateEloDisplay();

        // Initial data fetch with a slight delay to ensure everything is ready
        setTimeout(() => {
            fetchLeaderboard();
        }, 200);

        // Debug function to inspect current filter states (exposed globally for manual testing)
        function debugElementStates() {
            const checkedCountries = Array.from(document.querySelectorAll('input[name="countries"]:checked')).map(cb => cb.value);
            if (eloSlider && eloSlider.noUiSlider) {
                const [min, max] = eloSlider.noUiSlider.get().map(v => Math.round(v));
            }
        }

        window.debugLeaderboard = debugElementStates;
            window.testFetch = () => {
            debugElementStates();
            fetchLeaderboard();
        };

        // Add class indicating script loaded (can be used for styling or further scripts)
        document.body.classList.add('leaderboard-js-loaded');

    } catch (error) {
        // Show error details in the UI or alert if critical failure during init
        const errorMsg = `
        <div class="alert alert-danger my-3">
            <h4 class="alert-heading">JavaScript Error</h4>
            <p>${error.message}</p>
            <hr>
            <pre class="bg-dark text-white p-3 small">${error.stack}</pre>
        </div>`;

        if (document.querySelector('.leaderboard-table-box')) {
            document.querySelector('.leaderboard-table-box').innerHTML = errorMsg;
        } else {
            alert('Critical error initializing leaderboard: ' + error.message);
        }
    }
});
