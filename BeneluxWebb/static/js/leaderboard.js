// Debug mode can be toggled
const DEBUG_MODE = true;
const SHOW_DEBUG_ALERT = false;

// // Only show alert in debug mode and if enabled
// if (DEBUG_MODE && SHOW_DEBUG_ALERT) {
//     alert("Leaderboard JS file is loading!");
// }


document.addEventListener('DOMContentLoaded', () => {
    try {
        const eloSlider = document.getElementById('elo-slider');
        const form = document.getElementById('leaderboard-table-form');
        const tableContainer = document.querySelector('.leaderboard-table-box');
        const searchInput = document.getElementById('search');
        const perPageSelect = document.getElementById('per-page-select');

        if (!form) {
            alert('Critical error: leaderboard-table-form not found!');
            return;
        }

        if (!eloSlider) {
            alert('Critical error: elo-slider not found!');
            return; // Stop execution if slider is missing
        }
    let debounceTimer;

    // Load saved filters from localStorage
    function loadFilterState() {
        const saved = JSON.parse(localStorage.getItem('leaderboardFilters')) || {};
        
        if (saved.search) {
            searchInput.value = saved.search;
        }
        if (saved.per_page) {
            perPageSelect.value = saved.per_page;
        }

        document.querySelectorAll('input[name="countries"]').forEach(cb => {
            const shouldCheck = saved.countries?.includes(cb.value) ?? true;
            cb.checked = shouldCheck;
        });

        // If saved ELO range exists, set slider start values
        if (saved.min_elo !== undefined && saved.max_elo !== undefined) {
            eloSlider.noUiSlider.set([saved.min_elo, saved.max_elo]);
            updateEloValues(); // Update the display values
        }
        
        // Load saved sort and page (only if they exist)
        if (saved.sort) {
            form.dataset.sort = saved.sort;
        }
        if (saved.page && saved.page > 1) {
            form.dataset.page = saved.page;
        }
    }

    // Save current filters to localStorage
    function saveFilterState(min, max) {
        const countries = Array.from(document.querySelectorAll('input[name="countries"]:checked')).map(cb => cb.value);
        const filters = {
            search: searchInput.value,
            min_elo: min,
            max_elo: max,
            countries,
            per_page: perPageSelect.value,
            sort: form.dataset.sort,
            page: form.dataset.page
        };
        localStorage.setItem('leaderboardFilters', JSON.stringify(filters));
    }

    // Build query from current form and slider values
    function buildQueryParams() {
        const params = new URLSearchParams();
        
        // Add search term
        if (searchInput.value.trim()) {
            params.set('search', searchInput.value.trim());
        }
        
        // Add selected countries
        const selectedCountries = Array.from(document.querySelectorAll('input[name="countries"]:checked')).map(cb => cb.value);
        selectedCountries.forEach(country => {
            params.append('countries', country);
        });
        
        // Get slider values for ELO range
        const [minElo, maxElo] = eloSlider.noUiSlider.get().map(v => Math.round(v));
        params.set('min_elo', minElo);
        params.set('max_elo', maxElo);
        
        // Add per-page value
        params.set('per_page', perPageSelect.value);

        if (form.dataset.sort) {
            params.set('sort', form.dataset.sort);
        }
        if (form.dataset.page) {
            params.set('page', form.dataset.page);
        }
        
        const queryString = params.toString();
        return queryString;
    }

    // Fetch and update table with enhanced error handling
    async function fetchLeaderboard() {
        
        // Get the current ELO slider values (checking first if slider is initialized)
        let min = 0, max = 5000;
        if (eloSlider && eloSlider.noUiSlider) {
            [min, max] = eloSlider.noUiSlider.get().map(v => Math.round(v));
        }
        // Save state and build query
        saveFilterState(min, max);
        const query = buildQueryParams();
        const requestUrl = `/leaderboard?${query}`;
        
        try {
            // Show loading indicator in the table
            if (tableContainer) {
                tableContainer.innerHTML = '<div class="text-center my-5"><div class="spinner-border text-light" role="status"><span class="visually-hidden">Loading...</span></div><p class="text-light mt-2">Loading data...</p></div>';
            }
            
            // Make the AJAX request with proper headers
            const res = await fetch(requestUrl, {
                headers: { 
                    'X-Requested-With': 'XMLHttpRequest',
                    'Cache-Control': 'no-cache'
                }
            });
            
            // Check if we got a valid response
            if (!res.ok) {
                throw new Error(`HTTP error! status: ${res.status}`);
            }
            
            // Get the HTML content and log some debugging info
            const htmlContent = await res.text();
            
            // Update the table content
            if (tableContainer) {
                tableContainer.innerHTML = htmlContent;
                
                // Rebind events for the new content
                bindSortEvents();
                bindPaginationEvents();
                
            }
        } catch (error) {
            if (tableContainer) {
                tableContainer.innerHTML = `
                    <div class="alert alert-danger my-3">
                        <h4 class="alert-heading">Error loading data!</h4>
                        <p>${error.message}</p>
                        <hr>
                        <button class="btn btn-outline-danger" onclick="testFetch()">Retry</button>
                    </div>
                `;
            }
        }
    }

    // Bind sort
    function bindSortEvents() {
        const sortHeaders = document.querySelectorAll('.sort-header');
        sortHeaders.forEach(header => {
            header.addEventListener('click', e => {
                e.preventDefault();
                const sortValue = header.dataset.sort;
                form.dataset.sort = sortValue;
                form.dataset.page = 1;
                fetchLeaderboard();
            });
        });
    }

    // Bind pagination
    function bindPaginationEvents() {
        const paginationLinks = document.querySelectorAll('.pagination-link');
        paginationLinks.forEach(link => {
            link.addEventListener('click', e => {
                e.preventDefault();
                const pageValue = link.dataset.page;
                form.dataset.page = pageValue;
                fetchLeaderboard();
            });
        });
    }

    // Country filters
    const countryCheckboxes = document.querySelectorAll('input[name="countries"]');
    countryCheckboxes.forEach(cb => {
        cb.addEventListener('change', () => {
            form.dataset.page = 1;
            fetchLeaderboard();
        });
    });

    // Per-page selector
    perPageSelect.addEventListener('change', () => {
        form.dataset.page = 1; // Reset to first page when changing page size
        fetchLeaderboard();
    });

    // Debounced search - reduce timeout for more responsive typing
    searchInput.addEventListener('input', () => {
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(() => {
            form.dataset.page = 1;
            fetchLeaderboard();
        }, 150); // Reduced from 300ms to 150ms for more responsive typing
    });

    // Setup noUiSlider (only once)
    // Check if slider exists and isn't already initialized
    if (eloSlider && !eloSlider.noUiSlider) {
        
        // Check if noUiSlider is defined
        if (typeof noUiSlider === 'undefined') {
            throw new Error('noUiSlider library not loaded');
        }
        
        noUiSlider.create(eloSlider, {
            start: [0, 5000],
            connect: true,
            range: {
                min: 0,
                max: 5000
            },
            step: 50,
            behaviour: 'tap-drag',
            format: {
                to: function (value) {
                    return Math.round(value);
                },
                from: function (value) {
                    return Number(value);
                }
            }
        });
    }

    let sliderDebounceTimer;

    // Update ELO value displays
    function updateEloValues() {
        const [min, max] = eloSlider.noUiSlider.get().map(v => Math.round(v));
        document.getElementById('elo-min-value').textContent = min;
        document.getElementById('elo-max-value').textContent = max;
    }

    // Load saved filters after slider is created
    loadFilterState();

    // Update display values initially
    updateEloValues();

    eloSlider.noUiSlider.on('update', updateEloValues);
    eloSlider.noUiSlider.on('change', () => {
        clearTimeout(sliderDebounceTimer);
        sliderDebounceTimer = setTimeout(() => {
            form.dataset.page = 1;
            fetchLeaderboard();
        }, 200);
    });

    // Additional debugging function to check element states
    function debugElementStates() {
        const checkedCountries = Array.from(document.querySelectorAll('input[name="countries"]:checked')).map(cb => cb.value);
        
        if (eloSlider && eloSlider.noUiSlider) {
            const [min, max] = eloSlider.noUiSlider.get().map(v => Math.round(v));
        }
    }

    // Add debug function to window for manual testing
    window.debugLeaderboard = debugElementStates;
    
    // Test function to manually trigger a fetch (for debugging)
    window.testFetch = function() {
        debugElementStates();
        fetchLeaderboard();
    };
    
    // Add init success indicator to document
    document.body.classList.add('leaderboard-js-loaded');
    
    // Set a short timeout to ensure all elements are properly ready
    setTimeout(() => {
        // Call fetchLeaderboard without trying to use any return value
        fetchLeaderboard(); // Initial load
    }, 200);
    
    } catch (error) {
        // Show error in page instead of alert
        const errorMsg = `
            <div class="alert alert-danger my-3">
                <h4 class="alert-heading">JavaScript Error</h4>
                <p>${error.message}</p>
                <hr>
                <pre class="bg-dark text-white p-3 small">${error.stack}</pre>
            </div>
        `;
        
        // Try to show error in page
        if (tableContainer) {
            tableContainer.innerHTML = errorMsg;
        } else if (document.getElementById('status-messages')) {
            document.getElementById('status-messages').innerHTML = errorMsg;
        } else {
            // Last resort: alert
            alert('Critical error initializing leaderboard: ' + error.message);
        }
    }
});