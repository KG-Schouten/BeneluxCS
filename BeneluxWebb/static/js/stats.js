document.addEventListener('DOMContentLoaded', async () => {
    // Initialize bootstrap-select dropdowns
    $('.selectpicker').selectpicker();


    // Cache DOM references and variables
    const applyFiltersBtn = document.querySelector('.apply-button');
    let dataTable = null;         // will hold the DataTable instance
    let stat_field_names = [];    // list of stat column keys from server
    let columns_mapping = {};     // metadata for each column (title, decimals, etc.)
    let columns_perm = [];        // columns always visible (permanent)


    // Helper function: gather all selected filters into an object
    function collectFilterData() {
        const filters = {};

        // Collect checkbox groups
        const events = Array.from(document.querySelectorAll('input[name="events"]:checked')).map(el => el.value);
        if (events.length) filters.events = events.join(',');

        const countries = Array.from(document.querySelectorAll('input[name="countries"]:checked')).map(el => el.value);
        if (countries.length) filters.countries = countries.join(',');

        // Collect multiple select (Bootstrap selectpicker)
        const seasons = $('#seasons-select').val() || [];
        if (seasons.length) filters.seasons = seasons.join(',');

        const divisions = Array.from(document.querySelectorAll('input[name="divisions"]:checked')).map(el => el.value);
        if (divisions.length) filters.divisions = divisions.join(',');

        const stages = Array.from(document.querySelectorAll('input[name="stages"]:checked')).map(el => el.value);
        if (stages.length) filters.stages = stages.join(',');

        // Date range filter (using daterangepicker plugin)
        const datePicker = $('#dateFilter').data('daterangepicker');
        if (datePicker && datePicker.chosenLabel !== 'All Time') {
            filters.start_date = datePicker.startDate.format('YYYY-MM-DD');
            filters.end_date = datePicker.endDate.format('YYYY-MM-DD');
        }

        // Min/max maps filter
        const minVal = document.querySelector('.min-val');
        const maxVal = document.querySelector('.max-val');
        if (minVal && minVal.value !== minVal.min) filters.min_maps = minVal.value;
        if (maxVal && maxVal.value !== maxVal.max) filters.max_maps = maxVal.value;

        return filters;
    }


    // Enable accordion behavior for filter headers
    document.querySelectorAll('.filter-header').forEach(header => {
        header.addEventListener('click', () => header.parentElement.classList.toggle('open'));
    });


    // Initial metadata request to get available stat columns
    const initialFilters = collectFilterData();
    const queryParams = new URLSearchParams(initialFilters);
    const metaResp = await fetch(`/api/stats?${queryParams.toString()}`);
    const metaJson = await metaResp.json();


    // Store server-provided metadata
    stat_field_names = metaJson.stat_field_names;
    columns_mapping = metaJson.columns_mapping;
    columns_perm = metaJson.columns_perm;


    // Define core columns that are always shown
    const metaCols = [
        { 
            data: "player_name", 
            title: "Player", 
            name: "player_name",
            render: function(data, type, row) {
                if (type !== 'display') return data;

                // Get country flag
                let flagHtml = row.country 
                    ? `<img src="static/img/flags/${row.country.toLowerCase()}.png" 
                            alt="${row.country}" class="me-1" 
                            style="width:16px; height:12px;">` 
                    : '';

                // Add alias if exists
                let aliasHtml = (row.alias && row.alias.trim() !== '') 
                    ? ` <span class="text-muted">(${row.alias})</span>` 
                    : '';

                return `${flagHtml}${data}${aliasHtml}`;
            }
        },
        { data: "maps_played", title: "Maps", name: "maps_played" },
        { data: "map_win_pct", title: "Win %", name: "map_win_pct" }
    ];


    // Split stat fields into permanent + hidden groups
    const permCols = stat_field_names.filter(col => columns_perm.includes(col));
    const hiddenCols = stat_field_names.filter(col => !columns_perm.includes(col));


    // Helper: get display name or fallback to col
    const getName = col => columns_mapping[col]?.name || col;


    // Sort alphabetically by display name
    // permCols.sort((a, b) => getName(a).localeCompare(getName(b)));
    hiddenCols.sort((a, b) => getName(a).localeCompare(getName(b)));


    // Build column objects
    const statsCols = [...permCols, ...hiddenCols].map(col => ({
        data: col,
        title: getName(col),
        name: col,
        visible: columns_perm.includes(col),
        render: function(data, type) {
            if (type === 'display' || type === 'filter') {
                const round = columns_mapping[col]?.round ?? 2;
                return (data === null || data === undefined) ? '' : parseFloat(data).toFixed(round);
            }
            return data;
        }
    }));


    // Combine fixed + dynamic columns
    const allColumns = [...metaCols, ...statsCols];


    // Initialize the DataTable
    dataTable = $('#stats-data-table').DataTable({
        ajax: {
            url: `/api/stats?${queryParams.toString()}`, // initial data source
            dataSrc: 'data'
        },
        columns: allColumns,
        paging: true,
        searching: true,
        ordering: true,
        info: true,
        lengthMenu: [[10, 25, 50, -1], [10, 25, 50, "All"]],
        scrollX: true,
        stateSave: true,
        processing: true,
        fixedColumns: {
            start: 1
        }
    });


    // Create Dropdown and move it next to the search bar
    const targetDiv = document.querySelector(
        '.dt-layout-end'
    );
    const dropdown = document.createElement('div');
    dropdown.className = 'dropdown';
    dropdown.innerHTML = `
        <button class="btn btn-secondary dropdown-toggle bi bi-funnel-fill" type="button" id="columnsDropdown" data-bs-toggle="dropdown" aria-expanded="false" data-bs-auto-close="outside">
        </button>
        <ul class="dropdown-menu p-3" aria-labelledby="columnsDropdown" style="max-height: 500px; overflow-y: auto;">
            <!-- JS will fill checkboxes here -->
        </ul>
    `;
    targetDiv.appendChild(dropdown);


    // Once DataTable is initialized, build column toggle checkboxes
    dataTable.on('init.dt', function() {
        const dropdownMenu = document.querySelector('.dropdown-menu[aria-labelledby="columnsDropdown"]');
        if (!dropdownMenu) return;

        // Clear existing items
        dropdownMenu.innerHTML = '';

        // Only toggle columns that are NOT permanently visible
        let toggleable_cols = stat_field_names.filter(col => !columns_perm.includes(col));

        // Sort alphabetically by display name
        toggleable_cols.sort((a, b) => getName(a).localeCompare(getName(b)));

        toggleable_cols.forEach(colName => {
            const isVisible = dataTable.column(`${colName}:name`).visible();

            // Create list item with checkbox + label
            const li = document.createElement('li');
            li.innerHTML = `
                <div class="form-check">
                    <input class="form-check-input column-toggle" type="checkbox" value="${colName}" id="col_${colName}" ${isVisible ? 'checked' : ''}>
                    <label class="form-check-label" for="col_${colName}">
                        ${columns_mapping[colName]?.name || colName}
                    </label>
                </div>
            `;
            dropdownMenu.appendChild(li);
        });

        // Wire up checkbox listeners to toggle column visibility
        document.querySelectorAll('.column-toggle').forEach(cb => {
            cb.addEventListener('change', e => {
                const colName = e.target.value;
                dataTable.column(`${colName}:name`).visible(e.target.checked);
            });
        });
    });


    // Apply filters button reloads table data with new filters
    applyFiltersBtn.addEventListener('click', () => {
        const filters = collectFilterData();
        const qParams = new URLSearchParams(filters);
        dataTable.ajax.url(`/api/stats?${qParams.toString()}`).load();
    });

});
