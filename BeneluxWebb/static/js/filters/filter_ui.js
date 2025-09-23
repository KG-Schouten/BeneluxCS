document.addEventListener('DOMContentLoaded', function () {
    const filterContainers = document.querySelectorAll('.filter-container');
    const applyFiltersBtn = document.querySelector('.apply-button');
    const clearAllButton = document.querySelector('.clear-all-filters');

    // Enable accordion behavior for filter headers
    document.querySelectorAll('.filter-header').forEach(header => {
        header.addEventListener('click', () => header.parentElement.classList.toggle('open'));
    });

    function getSelectedCount(container) {
        const name = container.dataset.filterName;
        let count = 0;

        if (name === 'countries' || name === 'divisions' || name === 'stages') {
            count = container.querySelectorAll('input[type="checkbox"]:checked').length;
        } else if (name === 'seasons') {
            count = $(`#${name}-select`).val().length;
        } else if (name === 'events' || name === 'timestamp') {
            // Check if the selected radio is NOT the default one
            const selectedRadio = container.querySelector(`input[name="${name}"]:checked`);
            if (selectedRadio && !selectedRadio.hasAttribute('data-default')) {
                count = 1;
            }
        } else if (name === 'maps_played') {
            const minVal = container.querySelector('.min-val');
            const maxVal = container.querySelector('.max-val');
            if (minVal.value !== minVal.min || maxVal.value !== maxVal.max) {
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
        let totalActiveFilters = 0;
        filterContainers.forEach(container => {
            const indicator = container.querySelector('.filter-indicator');
            const count = getSelectedCount(container);

            if (count > 0) {
                indicator.textContent = count;
                indicator.style.display = 'flex';
                totalActiveFilters++;
            } else {
                indicator.style.display = 'none';
            }
        });

        if (totalActiveFilters > 0) {
            clearAllButton.disabled = false;
        } else {
            clearAllButton.disabled = true;
        }
    }

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
                delete inputBox.dataset.teamId; // remove stored id
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
                const teamIdsJson = JSON.stringify(opt.team_ids);  // must be array of arrays
                const avatar = opt.avatar || '/static/img/faceit/team_avatar_placeholder.jpg';
                return `
                    <li data-team-ids='${teamIdsJson}'>
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
                    inputBox.teamIds = JSON.parse(li.dataset.teamIds); // store selected team IDs
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
});
