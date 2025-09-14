document.addEventListener('DOMContentLoaded', function () {
    const filterContainers = document.querySelectorAll('.filter-container');
    const clearAllButton = document.querySelector('.clear-all-filters');

    function getSelectedCount(container) {
        const name = container.dataset.filterName;
        let count = 0;

        if (name === 'countries') {
            count = container.querySelectorAll('input[name="countries"]:checked').length;
        } else if (name === 'seasons') {
            count = $(`#${name}-select`).val().length;
        } else if (name === 'divisions' || name === 'stages') {
            count = container.querySelectorAll('input[type="checkbox"]:checked').length;
        } else if (name === 'timestamp') {
            const dateText = container.querySelector('.date_filter_start').textContent;
            if (dateText && dateText !== 'All Time') {
                count = 1;
            }
        } else if (name === 'maps_played') {
            const minVal = container.querySelector('.min-val');
            const maxVal = container.querySelector('.max-val');
            if (minVal.value !== minVal.min || maxVal.value !== maxVal.max) {
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

        if (name === 'countries') {
            container.querySelectorAll('input[name="countries"]:checked').forEach(c => c.checked = false);
        } else if (name === 'seasons') {
            $(`#${name}-select`).selectpicker('deselectAll');
        } else if (name === 'divisions' || name === 'stages') {
            container.querySelectorAll('input[type="checkbox"]:checked').forEach(c => c.checked = false);
        } else if (name === 'events') {
            container.querySelectorAll('input[type="radio"]').forEach(r => r.checked = false);
        } else if (name === 'timestamp') {
            $('#dateFilter').data('daterangepicker').setStartDate(moment('2000-01-01'));
            $('#dateFilter').data('daterangepicker').setEndDate(moment());
            $('#dateFilter .date_filter_label').text('All Time').show();
            $('#dateFilter .date_filter_start').html('');
            $('#dateFilter .date_filter_end').html('');
        } else if (name === 'maps_played') {
            const minVal = container.querySelector('.min-val');
            const maxVal = container.querySelector('.max-val');
            minVal.value = minVal.min;
            maxVal.value = maxVal.max;
            // Manually trigger input event to update UI
            minVal.dispatchEvent(new Event('input'));
            maxVal.dispatchEvent(new Event('input'));
        }
        updateIndicators();
    }

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

    // Specific listener for daterangepicker
    $('#dateFilter').on('apply.daterangepicker', updateIndicators);

    // Listener for the global clear all button
    clearAllButton.addEventListener('click', () => {
        filterContainers.forEach(container => {
            resetFilter(container);
        });
    });

    // Initial check
    updateIndicators();
});
