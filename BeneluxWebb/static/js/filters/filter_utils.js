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

                let ids = input.teamIds;

                if (!ids && input.dataset.teamIds) {
                    try {
                        ids = JSON.parse(input.dataset.teamIds);
                    } catch (e) {
                        console.error(`[${name}] Failed to parse teamIds from dataset`, e);
                    }
                }

                if (ids && Array.isArray(ids)) {
                    filters[`${name}_ids`] = JSON.stringify(ids);
                }
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

    return filters;
}