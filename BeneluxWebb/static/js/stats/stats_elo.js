document.addEventListener('DOMContentLoaded', async () => {
    const applyFiltersBtn = document.querySelector('.apply-button');
    let dataTable = null; 

    const queryParams = getParamsFromUrl();

    // Define core columns that are always shown
    const Cols = [
        {
            data: "rank", 
            title: "#", 
            name: "rank", 
            className: "text-center", 
            width: "120px",
            render: function(data, type, row) {
                if (type === 'display') {

                    const rankChangeWeek = row.history?.[row.history.length - 1]?.rank - row.history?.[0]?.rank;

                    // Determine rank change
                    let rankChangeHtml = '';
                    if (rankChangeWeek > 0) {
                        rankChangeHtml = `<span class="text-success"><i class="bi bi-chevron-double-up"></i> +${rankChangeWeek}</span>`;
                    } else if (rankChangeWeek < 0) {
                        rankChangeHtml = `<span class="text-danger"><i class="bi bi-chevron-double-down"></i> ${rankChangeWeek}</span>`;
                    } else {
                        rankChangeHtml = `<span class="text-muted"><i class="bi bi-dash-lg"></i></span>`;
                    }
                    
                    // Build the tooltip HTML
                    const historyHtml = (row.history || []).map(h => {
                        // Rank change for this row
                        let rankTooltipChange = '';
                        if (h.rank_change > 0) {
                            rankTooltipChange = `<span class="text-success">(+${h.rank_change})</span>`;
                        } else if (h.rank_change < 0) {
                            rankTooltipChange = `<span class="text-danger">(${h.rank_change})</span>`;
                        }

                        // Elo change for this row
                        let eloTooltipChange = '';
                        if (h.elo_change > 0) {
                            eloTooltipChange = `<span class="text-success">(+${h.elo_change})</span>`;
                        } else if (h.elo_change < 0) {
                            eloTooltipChange = `<span class="text-danger">(${h.elo_change})</span>`;
                        }

                        return `
                            <tr>
                                <td><div class="date-cell text-muted">${h.date}</div></td> 
                                <td><div class="rank-cell">${h.rank} ${rankTooltipChange}</div></td>
                                <td>${h.faceit_elo} ${eloTooltipChange}</td>
                            </tr>
                        `;
                    }).join('');

                    const tooltipContent = `
                        <div class="tooltip-content">
                            <table class="history-table">
                                <thead>
                                    <tr>
                                        <th></th>
                                        <th>Rank</th>
                                        <th>Elo</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${historyHtml || '<tr><td colspan="3">No historical data available.</td></tr>'}
                                </tbody>
                            </table>
                        </div>
                    `;


                    return `
                        <span class="rank-tooltip" data-tippy-content='${tooltipContent.trim()}'>
                            <div class="rank-badge">
                                <span class="rank-number">${data}</span>
                                <span class="rank-change-indicator">${rankChangeHtml}</span>
                            </div>
                        </span>
                    `;
                }
                return data;
            }
        },
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
                    let avatarHtml = row.avatar
                        ? `<img src="${row.avatar}" class="table-avatar me-1">`
                        : `<img src="/static/img/faceit/player_avatar_placeholder.jpg" class="table-avatar me-1">`;

                    let flagHtml = row.country 
                        ? `<img src="/static/img/flags/${row.country.toLowerCase()}.png" class="table-flag me-1">`
                        : '';
                    let nameHtml = `<span class="player-name">${data}</span>`;

                    let aliasHtml = row.alias ? ` <span class="text-muted">(${row.alias})</span>` : '';
                    return `
                        <a class='player-cell text-hover-table' href="https://www.faceit.com/en/players/${row.player_name}" target="_blank" rel="noopener noreferrer">        
                            ${avatarHtml}${flagHtml}${nameHtml}${aliasHtml}
                        </a>`;
                }

                if (type === 'sort') {
                    // Remove dashes, underscores, spaces, etc.
                    return data.replace(/[-_\s]/g, '').toLowerCase();
                }
                return data;
            }
        },
        {
            data: "faceit_elo", 
            title: "Elo", 
            name: "faceit_elo", 
            width: "150px",
            className: "highlight-cell",
            render: function(data, type, row) {
                if (type === 'display') {
                    const eloChangeWeek = (row.history?.[row.history.length - 1]?.faceit_elo - row.history?.[0]?.faceit_elo) * -1;
                    let changeHtml = '';
                    if (eloChangeWeek > 0) {
                        changeHtml = `<span class="elo-change-value text-success">+${eloChangeWeek}</span>`;
                    } else if (eloChangeWeek < 0) {
                        changeHtml = `<span class="elo-change-value text-danger">${eloChangeWeek}</span>`;
                    }

                    return `
                        <div class="elo-cell">
                            <span class="elo-value">${data}</span>
                            ${changeHtml}
                        </div>
                    `;
                }
                return data;
            }
        }
    ]

    dataTable = $('#stats-elo-data-table').DataTable({
        ajax: {
            url: `/api/stats/elo/data?${queryParams.toString()}`, // initial data source
            dataSrc: 'data'
        },
        columns: Cols,
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
        order: [[Cols.length - 1, 'desc']],
        info: true,
        processing: true,
        columnControl: ['orderStatus'],
        columnDefs: [
            {   
                target: -1,
                orderable: true,
                columnControl: ['orderStatus'],
                orderSequence: ['asc', 'desc']
            },
            {   
                target: '_all',
                orderable: false,
                columnControl: []
            }
        ],
        ordering: {
            indicators: false,
            handler: true, 
        },
        language: {
            search: "",
            searchPlaceholder: "Search players"
        },
        drawCallback: function() {
            // Destroy any previous tooltips to avoid duplicates
            if (this.tippyInstances) {
                this.tippyInstances.forEach(inst => inst.destroy());
            }

            // Initialize Tippy
            this.tippyInstances = tippy(this.api().table().body().querySelectorAll('.rank-tooltip'), {
                theme: 'table',
                allowHTML: true,
                interactive: true,
                placement: 'right',
                followCursor: 'vertical',
            });
        },
        initComplete: function(settings, json) {
            // Initialize selectpicker, but DOM already moved
            $('#pageLengthSelect').selectpicker();
            $('#pageLengthSelect').on('changed.bs.select', function () {
                const val = parseInt($(this).val(), 10);
                dataTable.page.len(val).draw();
            });
        }
    });

    // Move the page length selector into the DataTables wrapper
    const wrapper = $(dataTable.table().container());
    $('.page-length-selector').appendTo(wrapper.find('.dt-layout-start').first());

    // Add search icon to search input
    const searchContainer = $('#stats-elo-data-table_wrapper > div:first-child .dt-search');
    searchContainer.addClass('search-with-icon');
    $('<i class="bi bi-search search-icon"></i>').prependTo(searchContainer);
    

    // Function to apply filters and reload the table
    function applyAndReloadTable() {
        const filters = collectFilterData();
        const qParams = new URLSearchParams(filters);

        // Update URL
        const newUrl = `${window.location.pathname}?${qParams.toString()}`;
        window.history.pushState({path: newUrl}, '', newUrl);

        dataTable.ajax.url(`/api/stats/elo/data?${qParams.toString()}`).load();
    }

    applyFiltersBtn.addEventListener('click', applyAndReloadTable);

    // --- Listen for the custom event from filter_ui.js ---
    document.addEventListener('filtersCleared', applyAndReloadTable);

})