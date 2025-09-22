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

        // Sliders (assuming dual range)
        const minVal = container.querySelector('.min-val');
        const maxVal = container.querySelector('.max-val');

        console.log(`[${name}] Checking slider inputs`, { minVal, maxVal });

        if (minVal && maxVal) {
            const minAttr = parseFloat(minVal.min ?? "");
            const maxAttr = parseFloat(maxVal.max ?? "");

            console.log(`[${name}] Slider raw values`, { 
                minValue: minVal.value, 
                maxValue: maxVal.value, 
                minAttr, 
                maxAttr 
            });

            if (parseFloat(minVal.value) !== minAttr) {
                filters[`min_${name}`] = minVal.value;
                console.log(`[${name}] -> filters.min_${name} = ${minVal.value}`);
            }
            if (parseFloat(maxVal.value) !== maxAttr) {
                filters[`max_${name}`] = maxVal.value;
                console.log(`[${name}] -> filters.max_${name} = ${maxVal.value}`);
            }
        } else {
            console.log(`[${name}] No slider inputs found`);
        }


        // Search box
        const searchWrapper = container.querySelector('.search-box');
        console.log(`[${name}] Checking search box`, { searchWrapper });

        if (searchWrapper) {
            const input = searchWrapper.querySelector('input[type="text"]');
            console.log(`[${name}] Search input`, { input });

            if (input && input.value.trim() !== "") {
                filters[`${name}_name`] = input.value.trim();
                console.log(`[${name}] -> filters.${name}_name = ${input.value.trim()}`);

                let ids = input.teamIds;
                console.log(`[${name}] Initial teamIds`, ids);

                if (!ids && input.dataset.teamIds) {
                    try {
                        ids = JSON.parse(input.dataset.teamIds);
                        console.log(`[${name}] Parsed teamIds from dataset`, ids);
                    } catch (e) {
                        console.error(`[${name}] Failed to parse teamIds from dataset`, e);
                    }
                }

                if (ids && Array.isArray(ids)) {
                    filters[`${name}_ids`] = JSON.stringify(ids);
                    console.log(`[${name}] -> filters.${name}_ids = ${JSON.stringify(ids)}`);
                }
            } else {
                console.log(`[${name}] No search input value set`);
            }
        } else {
            console.log(`[${name}] No search box found`);
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