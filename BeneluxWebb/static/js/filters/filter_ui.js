document.addEventListener('DOMContentLoaded', function () {
    const filterWrappers = document.querySelectorAll('.filter-wrapper');
    const filterContainers = document.querySelectorAll('.filter-container');
    const clearAllButton = document.querySelector('.clear-all-filters');

    // Enable accordion behavior for filter headers
    document.querySelectorAll('.filter-header').forEach(header => {
        header.addEventListener('click', () => header.parentElement.classList.toggle('open'));
    });

    // Initialize search boxes
    document.querySelectorAll('.search-box').forEach(box => {
        const resultBox = box.querySelector('.result-box');
        const inputBox = box.querySelector('input[type="text"]');

        // Get filter options from data attribute
        const options = JSON.parse(resultBox.dataset.searchOptions || "[]");

        inputBox.addEventListener("keyup", () => {
            const input = inputBox.value.trim().toLowerCase();
            let result = [];

            if (input.length) {
                if (input.length) {
                    result = options.filter(opt =>
                        opt.team_name.toLowerCase().includes(input)
                    );
                }
            }

            display(result);
        });

        function display(result) {
            if (!result.length) {
                resultBox.innerHTML = "";
                return;
            }

            const content = result.map(opt => {
                const teamNames = JSON.stringify(opt.team_name);
                const avatar = opt.avatar || '/static/img/faceit/team_avatar_placeholder.jpg';
                return `
                    <li data-team-ids='${teamNames}'>
                        <img src="${avatar}" 
                            alt="Team Avatar"
                            onerror="this.onerror=null;this.src='/static/img/faceit/team_avatar_placeholder.jpg';">
                        <span>${opt.team_name}</span>
                    </li>`;
            }).join("");
            resultBox.innerHTML = `<ul>${content}</ul>`;

            resultBox.querySelectorAll("li").forEach(li => {
                li.addEventListener("click", () => {
                    inputBox.value = li.querySelector("span").textContent;
                    resultBox.innerHTML = "";
                });
            });
        }
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
