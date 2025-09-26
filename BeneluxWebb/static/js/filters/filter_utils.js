function collectFilterData() {
    const filters = {};

    document.querySelectorAll('.filter-container').forEach(container => {
        const name = container.dataset.filterName;
        if (!name) return;

        // Collect checkboxes and radios
        const checkedInputs = Array.from(
            container.querySelectorAll('input[type="checkbox"]:checked, input[type="radio"]:checked')
        );
        if (checkedInputs.length) {
            // Radio → single, Checkbox → multiple
            const values = checkedInputs.map(el => el.value);
            filters[name] = checkedInputs[0].type === 'radio' ? values[0] : values.join(',');
        }

        // Select (single or multiple)
        const select = container.querySelector('select');
        if (select) {
            const selected = $(select).val() || [];
            if (Array.isArray(selected) && selected.length) {
                filters[name] = selected.join(',');
            } else if (typeof selected === 'string' && selected.trim() !== '') {
                filters[name] = selected;
            }
        }

        // Sliders (multiple per container)
        container.querySelectorAll('.range-slider-container').forEach((sliderContainer, idx) => {
            const minVal = sliderContainer.querySelector('.min-val');
            const maxVal = sliderContainer.querySelector('.max-val');

            if (minVal && maxVal) {
                const minAttr = parseFloat(minVal.min ?? "");
                const maxAttr = parseFloat(maxVal.max ?? "");

                // Use an index or slider name to differentiate sliders in the same container
                const sliderKey = sliderContainer.dataset.sliderName || idx;

                if (parseFloat(minVal.value) !== minAttr) {
                    filters[`min_${sliderKey}`] = minVal.value;
                }
                if (parseFloat(maxVal.value) !== maxAttr) {
                    filters[`max_${sliderKey}`] = maxVal.value;
                }
            }
        });

        // Search box
        const searchWrapper = container.querySelector('.search-box');

        if (searchWrapper) {
            const input = searchWrapper.querySelector('input[type="text"]');

            if (input && input.value.trim() !== "") {
                filters[`${name}_name`] = input.value.trim();
            }
        } 
    });

    // Special case: timestamp → expand into start/end dates
    if (filters.timestamp) {
        const range = getDateRange(filters.timestamp);
        if (range) {
            filters.start_date = range.start;
            filters.end_date = range.end;
            delete filters.timestamp; // remove raw value if not needed
        }
    }
    // console.log("Collected filters:", filters);
    return filters;
}

function getSelectedCount(container) {
    const name = container.dataset.filterName;
    let count = 0;

    if (name === 'countries' || name === 'divisions' || name === 'stages') {
        count = container.querySelectorAll('input[type="checkbox"]:checked').length;
    } else if (name === 'seasons') {
        const select = $(`#${name}-select`);
        if (select.length) {
            const val = select.val();
            if (Array.isArray(val)) {
                count = val.length;
            }
        }
    } else if (name === 'events' || name === 'timestamp') {
        // Check if the selected radio is NOT the default one
        const selectedRadio = container.querySelector(`input[name="${name}"]:checked`);
        if (selectedRadio && !selectedRadio.hasAttribute('data-default')) {
            count = 1;
        }
    } else if (name === 'maps_played') {
        const minVal = container.querySelector('.min-val');
        const maxVal = container.querySelector('.max-val');
        if (minVal && maxVal && (minVal.value !== minVal.min || maxVal.value !== maxVal.max)) {
            count = 1;
        }
    } else if (name === 'teams') {
        const inputBox = container.querySelector('input[type="text"]');
        if (inputBox && inputBox.value.trim() !== "") {
            count = 1;
        }
    }
    return count;
}

function updateIndicators() {
    const filterContainers = document.querySelectorAll('.filter-container');
    const clearAllButton = document.querySelector('.clear-all-filters');
    if (!filterContainers.length) return;

    let totalActiveFilters = 0;
    filterContainers.forEach(container => {
        const indicator = container.querySelector('.filter-indicator');
        if (!indicator) return;

        const count = getSelectedCount(container);

        if (count > 0) {
            indicator.textContent = count;
            indicator.style.display = 'flex';
            totalActiveFilters++;
        } else {
            indicator.style.display = 'none';
        }
    });

    if (clearAllButton) {
        if (totalActiveFilters > 0) {
            clearAllButton.disabled = false;
        } else {
            clearAllButton.disabled = true;
        }
    }
}

function getParamsFromUrl() {
    // Fill with parameters from filters
    const filters = collectFilterData();

    const urlParams = new URLSearchParams(window.location.search);

    Object.entries(filters).forEach(([key, value]) => {
        if (!urlParams.has(key) || urlParams.get(key) === "") {
            // only fill if missing or empty
            if (value !== null && value !== undefined) {
                urlParams.set(key, value);
            }
        }
    });

    return urlParams;
}

function updateURL(qParams) {
    const newUrl = `${window.location.pathname}?${qParams.toString()}`;
    window.history.pushState({path: newUrl}, '', newUrl);
}

function applyFiltersFromUrl() {
    const params = getParamsFromUrl();

    updateURL(params); // Ensure URL is in sync

    if (params.toString() === '') return;

    document.querySelectorAll('.filter-container').forEach(container => {
        const name = container.dataset.filterName;
        if (!name) return;

        // --- Checkboxes / Radios ---
        if (params.has(name)) {
            const values = params.get(name).split(',');
            container.querySelectorAll('input[type="checkbox"], input[type="radio"]').forEach(input => {
                input.checked = values.includes(input.value);
            });
        }

        // --- Selectpicker ---
        const select = container.querySelector('select');
        if (params.has(name) && select) {
            const values = params.get(name).split(',');
            $(`#${select.id}`).selectpicker('val', values);
            $(`#${select.id}`).selectpicker('refresh'); // Make sure styling updates
        }

        // --- Sliders ---
        container.querySelectorAll('.range-slider-container').forEach(sliderContainer => {
            const sliderKey = sliderContainer.dataset.sliderName;
            if (!sliderKey) return;

            const minKey = `min_${sliderKey}`;
            const maxKey = `max_${sliderKey}`;

            const minVal = sliderContainer.querySelector('.min-val');
            const maxVal = sliderContainer.querySelector('.max-val');

            if (params.has(minKey)) {
                minVal.value = params.get(minKey);
                minVal.dispatchEvent(new Event('input')); // triggers UI update
            }

            if (params.has(maxKey)) {
                maxVal.value = params.get(maxKey);
                maxVal.dispatchEvent(new Event('input'));
            }
        });

        // --- Search box ---
        if (params.has(`${name}_name`)) {
            const input = container.querySelector('.search-box input[type="text"]');
            if (input) {
                input.value = params.get(`${name}_name`);
                const event = new Event('input'); 
                input.dispatchEvent(event); // trigger live search highlight if any
            }
        }
    });

    // --- Update filter indicators / styling ---
    updateIndicators();  // updates counts, visibility, Clear All button
}