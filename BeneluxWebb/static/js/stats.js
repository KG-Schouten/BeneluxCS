document.addEventListener('DOMContentLoaded', async () => {
    // Cache DOM references and variables
    const applyFiltersBtn = document.querySelector('.apply-button');
    let dataTable = null;         // will hold the DataTable instance

    // Helper function: gather all selected filters into an object
    function collectFilterData() {
        const filters = {};

        // Collect checkbox groups
        const events = Array.from(document.querySelectorAll('input[name="events"]:checked')).map(el => el.value);
        if (events.length) filters.events = events.join(',');

        const countries = Array.from(document.querySelectorAll('input[name="countries"]:checked')).map(el => el.value);
        if (countries.length) filters.countries = countries.join(',');

        const seasons = $('#seasons-select').val() || [];
        if (seasons.length) filters.seasons = seasons.join(',');

        const divisions = Array.from(document.querySelectorAll('input[name="divisions"]:checked')).map(el => el.value);
        if (divisions.length) filters.divisions = divisions.join(',');

        const stages = Array.from(document.querySelectorAll('input[name="stages"]:checked')).map(el => el.value);
        if (stages.length) filters.stages = stages.join(',');

        // Handle timestamp → convert label into start/end dates
        const timestampOption = document.querySelector('input[name="timestamp"]:checked');
        if (timestampOption) {
            const range = getDateRange(timestampOption.value);
            if (range) {
                filters.start_date = range.start;
                filters.end_date = range.end;
            }
        }

        // Min/max maps filter
        const minVal = document.querySelector('.min-val');
        const maxVal = document.querySelector('.max-val');
        if (minVal && minVal.value !== minVal.min) filters.min_maps = minVal.value;
        if (maxVal && maxVal.value !== maxVal.max) filters.max_maps = maxVal.value;

        // Team search
        const teamBoxWrapper = document.querySelector('.search-box[data-search-name="teams"]');
        if (teamBoxWrapper) {
            const teamBox = teamBoxWrapper.querySelector('input[type="text"]');
            if (teamBox && teamBox.value.trim() !== "") {
                filters.team_name = teamBox.value.trim();

                let teamIds = teamBox.teamIds;
                if (!teamIds && teamBox.dataset.teamIds) {
                    try {
                        teamIds = JSON.parse(teamBox.dataset.teamIds);
                    } catch(e) {
                        console.error("Failed to parse teamIds from dataset", e);
                    }
                }

                if (teamIds && Array.isArray(teamIds)) {
                    filters.team_ids = JSON.stringify(teamIds);
                }
            }
        }
        return filters;
    }


    // Enable accordion behavior for filter headers
    document.querySelectorAll('.filter-header').forEach(header => {
        header.addEventListener('click', () => header.parentElement.classList.toggle('open'));
    });


    // Gather variables from flask api
    try {
        const response = await fetch('/api/stats/fields');
        const meta = await response.json();
        var stat_field_names = meta.stat_field_names || [];
        var columns_perm = meta.columns_perm || [];
        var columns_mapping = meta.columns_mapping || {};
    } catch (error) {
        console.error("Error fetching stats metadata:", error);
        var stat_field_names = [];
        var columns_perm = [];
        var columns_mapping = {};
    }


    // Initial page load
    const initialFilters = collectFilterData();
    const queryParams = new URLSearchParams(initialFilters);


    // Define core columns that are always shown
    const metaCols = [
        {
            data: "player_name",
            title: "Player",
            name: "player_name",
            render: function(data, type, row) {
                if (type === 'filter') {
                    // Combine name + alias
                    let text = `${data} ${row.alias || ''}`;

                    // Make a version where 1 → i and i → 1
                    const altText = text
                        .replace(/1/g, 'i')    // convert 1 → i

                    // Include both in the filter string
                    return `${text} ${altText}`;
                }

                if (type === 'display') {
                    let flagHtml = row.country 
                        ? `<img src="static/img/flags/${row.country.toLowerCase()}.png" class="me-1" style="width:16px; height:12px;">`
                        : '';
                    let aliasHtml = row.alias ? ` <span class="text-muted">(${row.alias})</span>` : '';
                    return `${flagHtml}${data}${aliasHtml}`;
                }

                if (type === 'sort') {
                    // Remove dashes, underscores, spaces, etc.
                    return data.replace(/[-_\s]/g, '').toLowerCase();
                }

                return data; // for filter/search
            }
        },
        { data: "maps_played", title: "Maps", name: "maps_played", searchable: false},
        { data: "map_win_pct", title: "Win %", name: "map_win_pct", searchable: false}
    ];


    // Split stat fields into permanent + hidden groups
    const permCols = stat_field_names.filter(col => columns_perm.includes(col));
    const hiddenCols = stat_field_names.filter(col => !columns_perm.includes(col));


    // Helper: get display name or fallback to col
    const getName = col => columns_mapping[col]?.name || col;


    // Sort alphabetically by display name
    hiddenCols.sort((a, b) => getName(a).localeCompare(getName(b)));


    // Build column objects
    const statsCols = [...permCols, ...hiddenCols].map(col => ({
        data: col,
        title: getName(col),
        name: col,
        visible: columns_perm.includes(col),
        searchable: false,
        render: function(data, type, row) {
            if (data === null || data === undefined) return '';

            const mapping = columns_mapping[col] || {};
            const round = mapping.round ?? 2;
            const value = parseFloat(data).toFixed(round);

            if (type === 'display') {
                // Apply styling if thresholds exist
                if (mapping.good !== undefined && mapping.bad !== undefined) {
                    if (parseFloat(data) >= mapping.good) {
                        return `<span class="text-success fw-bold">${value}</span>`;
                    } else if (parseFloat(data) <= mapping.bad) {
                        return `<span class="text-danger fw-bold">${value}</span>`;
                    }
                }
                return value; // normal styling
            }

            // For sort/filter use raw numeric value
            return parseFloat(data);
        }
    }));


    // Combine fixed + dynamic columns
    const allColumns = [...metaCols, ...statsCols];

    // Initialize the DataTable
    dataTable = $('#stats-data-table').DataTable({
        ajax: {
            url: `/api/stats/data?${queryParams.toString()}`, // initial data source
            dataSrc: 'data'
        },
        columns: allColumns,
        paging: true,
        lengthChange: false,
        lengthMenu: [[25, 50, -1], [25, 50, "All"]],
        pageLength: 25, 
        searching: true,
        search: {
            smart: true,
            regex: false,
            caseInsensitive: true,
        },
        order: [[1, 'desc']], // default sort by Maps played
        info: true,
        scrollX: true,
        processing: true,
        fixedColumns: {
            start: 1
        },
        columnControl: ['orderStatus', 'searchDropdown'],
        columnDefs: [
            {   
                // Target all but first column
                targets: 0,
                orderSequence: ["asc", "desc", ""]
            },
            {   
                targets: '_all',
                orderSequence: ["desc", "asc", ""]
            },
            {
                targets: [0,1],
                columnControl: ['order']
            }
        ],
        ordering: {
            indicators: false,
            handler: true, 
        },
        language: {
            search: "",
            searchPlaceholder: "Search players"
        }
    });
    
    // Page length select
    $('.page-length-selector').appendTo(
        '#stats-data-table_wrapper .dt-layout-start'
    );
    $('#pageLengthSelect').selectpicker();
    $('#pageLengthSelect').on('changed.bs.select', function () {
        const val = parseInt($(this).val(), 10);
        dataTable.page.len(val).draw();
    });


    // Add search icon to search input
    const searchContainer = $('#stats-data-table_wrapper > div:first-child .dt-search');
    searchContainer.addClass('search-with-icon');
    $('<i class="bi bi-search search-icon"></i>').prependTo(searchContainer);


    // Create a Buttons instance
    new DataTable.Buttons(dataTable, {
        buttons: [
            {
                extend: 'csv',
                text: "<i class='bi bi-file-earmark-spreadsheet-fill'></i><span class='csv-text'>Export CSV</span>",
                className: 'csv-btn'
            }, 
            {
                extend: 'colvis',
                text: '<i class="bi bi-funnel-fill"></i>',
                columns: ':not(:first-child):not(:nth-child(2))',
                popoverTitle: 'Toggle column visibility',
                postfixButtons: ['colvisRestore']
            }
        ]
    });
    dataTable.buttons().container().appendTo('.stats-wrapper #stats-data-table_wrapper > div:first-child .dt-layout-end');


    // Function to apply filters and reload the table
    function applyAndReloadTable() {
        const filters = collectFilterData();
        const qParams = new URLSearchParams(filters);
        dataTable.ajax.url(`/api/stats/data?${qParams.toString()}`).load();
    }

    // Apply filters button now uses the reusable function
    applyFiltersBtn.addEventListener('click', applyAndReloadTable);

    // --- Listen for the custom event from filter_ui.js ---
    document.addEventListener('filtersCleared', applyAndReloadTable);

});
