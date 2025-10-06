function collectDefaultFilters() {
    const defaults = {};

    document.querySelectorAll('.filter-container').forEach(container => {
        const name = container.dataset.filterName;
        if (!name) return;

        try {
            // Parse the initial default values
            const value = JSON.parse(container.dataset.default);
            defaults[name] = value;
        } catch (err) {
            console.warn(`Could not parse default value for filter "${name}":`, container.dataset.default);
            defaults[name] = [];
        }
    });
    return defaults;
}

function collectCurrentFilters() {
    const filters = {};

    document.querySelectorAll('.filter-container').forEach(container => {
        const name = container.dataset.filterName;
        if (!name) return;

        if (container.classList.contains("disabled")) {
            return;
        }

        try {
            // Parse the current values
            const value = JSON.parse(container.dataset.value);
            filters[name] = value;
        } catch (err) {
            console.warn(`Could not parse current value for filter "${name}":`, container.dataset.value);
        }
    });
    return filters;
}

function collectParamsFromUrl() {
    const params = new URLSearchParams(window.location.search);
    return params;
}

function collectFiltersFromUrl() {
    const params = collectParamsFromUrl();
    const filters = {};

    for (const [key, value] of params.entries()) {
        if (!value) {
            // Empty string → empty array
            filters[key] = [];
        } else if (value.includes(',')) {
            // Comma-separated values → array
            filters[key] = value.split(',').map(v => v.trim());
        } else {
            // Single value → wrap in array
            filters[key] = [value];
        }
    }

    return filters;
}


function getSelectedCount() {
    const currentFilters = collectCurrentFilters();
    const defaultFilters = collectDefaultFilters();
    const urlFilters = collectFiltersFromUrl();

    const isDisabled = (key) => {
        const container = document.querySelector(`[data-filter-name="${key}"]`);
        return container && container.classList.contains("disabled");
    };

    const normalizeArray = (val) => (Array.isArray(val) ? val : []);

    const countDifferences = (arr1, arr2) => {
        const set1 = new Set(arr1);
        const set2 = new Set(arr2);
        
        if (set1.size > set2.size) {
            return arr1.filter(v => !set2.has(v)).length;
        } else if (set2.size < set2.size) {
            return arr2.filter(v => !set1.has(v)).length;
        } else {
            return arr2.filter(v => !set1.has(v)).length
        }
    };

    const computeCounts = (sourceObj, compareObj) => {
        const counts = {};
        Object.entries(sourceObj).forEach(([key, sourceVals]) => {
            if (isDisabled(key)) {
                counts[key] = 0;
                return;
            }
            const compareArr = normalizeArray(compareObj[key]);
            counts[key] = countDifferences(sourceVals, compareArr);
        });
        return counts;
    };

    const currentFromDefaultCounts = computeCounts(defaultFilters, currentFilters);
    const currentFromUrlCounts = computeCounts(urlFilters, currentFilters);
    const urlFromDefaultCounts = computeCounts(defaultFilters, urlFilters);

    return { currentFromDefaultCounts, currentFromUrlCounts, urlFromDefaultCounts };
}

function updateIndicators() {
    console.log("--- Updating filter indicators ---");
    const filterWrappers = document.querySelectorAll('.filter-wrapper');
    const clearAllButton = document.querySelector('.clear-all-filters');
    const applyFiltersButton = document.querySelector('.apply-button');

    if (!filterWrappers.length) return;

    const { currentFromDefaultCounts, currentFromUrlCounts, urlFromDefaultCounts } = getSelectedCount();

    let clearCount = 0;
    let applyCount = 0;

    filterWrappers.forEach(wrapper => {
        const indicator = wrapper.querySelector('.filter-indicator');
        if (!indicator) return;

        if (wrapper.classList.contains("disabled")) {
            indicator.style.display = 'none';
            return;
        }

        const container = wrapper.querySelector('.filter-container');
        if (!container) return;

        const name = container.dataset.filterName;
        if (!name) return;

        const curFromDef = currentFromDefaultCounts[name] || 0;
        const urlFromDef = urlFromDefaultCounts[name] || 0;
        const curFromUrl = currentFromUrlCounts[name] || 0;

        // Show/hide indicator
        indicator.style.display = curFromDef > 0 ? 'flex' : 'none';
        if (curFromDef > 0) indicator.textContent = curFromDef;

        // Count for buttons
        if (curFromDef > 0 || urlFromDef > 0) clearCount++;
        if (curFromUrl > 0) applyCount++;
    });

    // Enable/disable buttons
    if (applyFiltersButton) applyFiltersButton.disabled = applyCount === 0;
    if (clearAllButton) clearAllButton.disabled = clearCount === 0;
}


function updateUrl() {
    const filterWrappers = document.querySelectorAll('.filter-wrapper');
    const currentFilters = collectCurrentFilters();
    const defaultFilters = collectDefaultFilters();

    // Fill currentFilters with defaults where missing
    Object.entries(defaultFilters).forEach(([key, value]) => {
        // If the value is not missing or undefined or an empty array/string
        if (
            value === undefined ||
            value === null ||
            (Array.isArray(value) && value.length === 0) ||
            (typeof value === "string" && value.trim() === "")
        ) {
            return; // skip this entry
        }

        if (!currentFilters.hasOwnProperty(key) || currentFilters[key] === null || currentFilters[key] === undefined || currentFilters[key] === "") {
            currentFilters[key] = value;
        }
    });

    // Convert currentFilters to URLSearchParams
    const qParams = new URLSearchParams();
    Object.entries(currentFilters).forEach(([key, value]) => {
        // If the filterWrapper with corresponding data-filter-name is disabled, skip it
        const wrapper = Array.from(filterWrappers).find(w => w.dataset.filterName === key);

        if (wrapper && wrapper.classList.contains("disabled")) {
            qParams.set(key, "");
            return;
        } else {
            if (value !== null && value !== undefined && value !== "") {
                qParams.set(key, value);
            }
        }
    });
    console.log("Updating URL with filters:", currentFilters);

    // Update the browser URL without reloading the page
    const newUrl = `${window.location.pathname}?${qParams.toString()}`;
    window.history.pushState({path: newUrl}, '', newUrl);
}

function applyFilters(filters) {
    document.querySelectorAll('.filter-container').forEach(container => {
        const name = container.dataset.filterName;
        if (!name) return;

        if (filters.hasOwnProperty(name)) {
            container.dataset.value = JSON.stringify(filters[name]);

            container.dispatchEvent(new CustomEvent('applyValuesFromUrl', {
                detail: filters[name],
                bubbles: true
            }));
        }
    });

    updateUrl();
    updateIndicators();
}

function applyFiltersFromUrl() {
    console.log("--- Applying filters from URL ---");
    const filtersURL = collectFiltersFromUrl();
    const defaultFilters = collectDefaultFilters();
    const urlParams = new URLSearchParams(window.location.search);

    // Fill filtersURL with defaults only for parameters NOT present in URL
    Object.entries(defaultFilters).forEach(([key, value]) => {
        // If the value is not missing or undefined or an empty array/string
        if (
            value === undefined ||
            value === null ||
            (Array.isArray(value) && value.length === 0) ||
            (typeof value === "string" && value.trim() === "")
        ) {
            return; // skip this entry
        }

        // Only add default if the parameter wasn't in the URL at all
        if (!urlParams.has(key) && !filtersURL.hasOwnProperty(key)) {
            filtersURL[key] = value;
        }
    });

    console.log("Applying filters from URL (with defaults):", filtersURL);

    applyFilters(filtersURL);
}


function resetFilters() {
    const defaultFilters = collectDefaultFilters();

    console.log(`Resetting filters to default:`, defaultFilters);

    applyFilters(defaultFilters);
}


// Apply filters on table
function applyFiltersAndReloadTable({ table, endpoint, filters = null }) {
    console.log("--- Applying filters and reloading table ---");
    if (!table || !endpoint) return;

    const appliedFilters = filters || collectCurrentFilters();

    plausible('Filters Applied', {
        props: {
            filters: Object.keys(appliedFilters).join(", ")
        }
    });


    // Update URL in browser
    applyFilters(appliedFilters);

    // Convert filters to URL params
    const qParams = collectParamsFromUrl()
    
    table.ajax.url(`${endpoint}?${qParams.toString()}`).load();
}


function observeFilterValueChanges(callback) {
    if (typeof callback !== 'function') {
        console.warn('observeFilterValueChanges requires a callback function');
        return [];
    }

    const containers = document.querySelectorAll('.filter-container');
    const observers = [];

    containers.forEach(container => {
        const observer = new MutationObserver(mutationsList => {
            for (const mutation of mutationsList) {
                if (mutation.type === 'attributes' && mutation.attributeName === 'data-value') {
                    const newValue = container.dataset.value;
                    callback(container, newValue);
                }
            }
        });

        observer.observe(container, { attributes: true });
        observers.push(observer);
    });

    return observers;
}


function handleEventsFilterChange(value) {
    const rules = {
        "all": [],
        "esea": [],
        "hub": ["seasons", "divisions", "stages", "teams"],
    };

    // Disable the ones listed in rules[value]
    console.log(`Disabling filters based on events selection "${value}":`, rules[value] || []);
    const toDisable = rules[value] || [];
    document.querySelectorAll('.filter-wrapper').forEach(container => {
        const name = container.dataset.filterName;
        if (!name) return;
        if (toDisable.includes(name)) {
            container.classList.add("disabled");
            // Also if class is open, close it
            container.classList.remove("open");
        } else {
            container.classList.remove("disabled");
        }
    });
}