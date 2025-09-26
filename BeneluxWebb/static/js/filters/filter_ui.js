document.addEventListener('DOMContentLoaded', function () {
    const filterContainers = document.querySelectorAll('.filter-container');
    const clearAllButton = document.querySelector('.clear-all-filters');

    // Enable accordion behavior for filter headers
    document.querySelectorAll('.filter-header').forEach(header => {
        header.addEventListener('click', () => header.parentElement.classList.toggle('open'));
    });

    function resetFilter(container) {
        const name = container.dataset.filterName;

        if (name === 'countries' || name === 'divisions' || name === 'stages') {
            container.querySelectorAll('input[type="checkbox"]:checked').forEach(c => c.checked = false);
        } else if (name === 'seasons') {
            $(`#${name}-select`).selectpicker('deselectAll');
        } else if (name === 'events' || name === 'timestamp') {
            // Find and check the default radio button
            const defaultRadio = container.querySelector('input[type="radio"][data-default="true"]');
            if (defaultRadio) {
                defaultRadio.checked = true;
            }
        } else if (name === 'maps_played') {
            const minVal = container.querySelector('.min-val');
            const maxVal = container.querySelector('.max-val');
            minVal.value = minVal.min;
            maxVal.value = maxVal.max;
            // Manually trigger input event to update UI
            minVal.dispatchEvent(new Event('input'));
            maxVal.dispatchEvent(new Event('input'));
        } else if (name === 'teams') {
            const inputBox = container.querySelector('input[type="text"]');
            if (inputBox) {
                inputBox.value = "";
            }
            const resultBox = container.querySelector('.result-box');
            if (resultBox) resultBox.innerHTML = "";
        }

        updateIndicators(); 
    }


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

    // Event Listeners
    filterContainers.forEach(container => {
        // Listen for changes within the filter body
        const body = container.querySelector('.filter-body');
        body.addEventListener('change', updateIndicators);

        // Special listener for search box input
        const searchInput = container.querySelector('.search-box input[type="text"]');
        if (searchInput) {
            searchInput.addEventListener('input', updateIndicators);
        }
    });
    

    // Specific listener for range slider
    document.querySelectorAll('.min-val, .max-val').forEach(slider => {
        slider.addEventListener('input', updateIndicators);
    });


    // Listener for the global clear all button
    clearAllButton.addEventListener('click', () => {
        filterContainers.forEach(container => {
            resetFilter(container);
        });
        document.dispatchEvent(new CustomEvent('filtersCleared'));
    });


    // Initial check
    updateIndicators();


    // Apply filters from URL on page load
    applyFiltersFromUrl();
});
