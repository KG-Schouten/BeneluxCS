document.addEventListener('DOMContentLoaded', function () {
    const clearAllButton = document.querySelector('.clear-all-filters');

    // Enable accordion behavior for filter headers
    document.querySelectorAll('.filter-header').forEach(header => {
        header.addEventListener('click', () => header.parentElement.classList.toggle('open'));
    });

    // Initialize search box for team search
    document.querySelectorAll('.search-box[data-filter-name="teams"]')
    .forEach(container => {
        container.addEventListener("search:update", e => {
            const { inputValue, options, results, setValue, selectedValues, multiselect } = e.detail;

            console.log("=== search:update event ===");
            console.log("Input value:", inputValue);
            console.log("Multiselect enabled:", multiselect);
            console.log("Currently selected values:", selectedValues);
            console.log("Total options:", options.length);

            results.innerHTML = "";
            if (!inputValue) {
                console.log("Input empty, clearing results.");
                return;
            }

            const filtered = options.filter(opt =>
                opt.team_name.toLowerCase().includes(inputValue.toLowerCase())
            );

            console.log("Filtered results:", filtered.map(o => o.team_name));

            if (filtered.length) {
                const ul = document.createElement("ul");

                filtered.forEach(opt => {
                    const li = document.createElement("li");

                    const img = document.createElement("img");
                    img.src = opt.avatar || "/static/img/faceit/team_avatar_placeholder.jpg";
                    img.alt = "Team Avatar";
                    img.onerror = () => {
                        console.warn("Image failed to load for team:", opt.team_name);
                        img.src = "/static/img/faceit/team_avatar_placeholder.jpg";
                    };

                    const span = document.createElement("span");
                    span.textContent = opt.team_name;

                    // If multiselect, mark already selected
                    if (multiselect && selectedValues.includes(opt.team_name)) {
                        li.classList.add("selected");
                        console.log("Marking as selected:", opt.team_name);
                    }

                    li.appendChild(img);
                    li.appendChild(span);

                    li.addEventListener("click", () => {
                        console.log("Clicked team:", opt.team_name);
                        setValue(opt.team_name);
                        console.log("Updated selected values:", container.dataset.value);
                    });

                    ul.appendChild(li);
                });

                results.appendChild(ul);
                console.log("Results DOM updated.");
            } else {
                console.log("No matches found for input:", inputValue);
            }
        });
    });



    
    const lastValues = {}; // store last value per filter
    const debounceTimers = {}; // store timer per filter
    observeFilterValueChanges((container, newValue) => {
        const filterName = container.dataset.filterName;
        if (!filterName) return;

        // Initialize last value and timer for this filter if needed
        if (!lastValues.hasOwnProperty(filterName)) lastValues[filterName] = null;
        if (!debounceTimers.hasOwnProperty(filterName)) debounceTimers[filterName] = null;

        clearTimeout(debounceTimers[filterName]);
        debounceTimers[filterName] = setTimeout(() => {
            if (newValue === lastValues[filterName]) return; // ignore duplicates
            lastValues[filterName] = newValue;

            if (filterName === 'events') {
                console.log('=-=-=-= Events filter changed, updating dependent filters...');
                handleEventsFilterChange(JSON.parse(newValue) || 'all');
            }

            updateIndicators();
        }, 10);
    });

    
    // Listener for the global clear all button
    clearAllButton.addEventListener('click', () => {
        resetFilters();
        document.dispatchEvent(new CustomEvent('filtersCleared'));
    });
});
