// Initialize all DataTables
export function initDataTables(context = document, columns_mapping = {}) {
    const teamId = context.dataset.teamId;
    const input = document.getElementById(`player-stats-data-${teamId}`);
    const tableId = `#player-stats-table-${teamId}`;
    const playerStats = JSON.parse(input.dataset.stats);
    const mainPlayers = JSON.parse(input.dataset.players);

    // Pre-sort playerStats so main players appear first
    const mainPlayerIds = new Set(mainPlayers.map(p => p.player_id));
    playerStats.sort((a, b) => {
        const aIsMain = mainPlayerIds.has(a.player_id);
        const bIsMain = mainPlayerIds.has(b.player_id);

        // If one is main and the other is not, main comes first
        if (aIsMain && !bIsMain) return -1;
        if (!aIsMain && bIsMain) return 1;

        // Both in same group: sort by hltv descending
        return (b.hltv || 0) - (a.hltv || 0);
    });


    const columns = [
        {
            data: "player_name",
            title: "Player",
            render: function (data, type, row) {
            if (type === 'display') {
                const flagHtml = row.country ? `<img src="/static/img/flags/${row.country.toLowerCase()}.png" class="table-flag me-1">` : '';
                return `
                <a class='player-cell text-hover-table' href="https://www.faceit.com/en/players/${row.player_name}" target="_blank" rel="noopener noreferrer">
                    ${flagHtml}<span class="player-name">${data}</span>
                </a>`;
            }
            return data;
            }
        },
        { data: "maps_played", title: "Maps"},
        { data: "headshots_percent", title: "HS%", render: data => data ? parseFloat(data).toFixed(0) : '0'},
        { data: "k_d_ratio", title: "K/D", render: data => data ? parseFloat(data).toFixed(2) : '0.00'},
        { 
            data: "hltv", 
            title: "HLTV", 
            render: function(data, type) {
            const mapping = columns_mapping['hltv'] || {};
            const round = mapping.round ?? 2;
            const value = parseFloat(data).toFixed(round);

            if (type === 'display') {
                if (mapping.good !== undefined && mapping.bad !== undefined) {
                if (parseFloat(data) >= mapping.good) {
                    return `<span class="text-success fw-bold">${value}</span>`;
                } else if (parseFloat(data) <= mapping.bad) {
                    return `<span class="text-danger fw-bold">${value}</span>`;
                }
                }
                return value;
            }
            return parseFloat(data);
            }
        }
    ];

    $(tableId).DataTable({
        data: playerStats,
        columns: columns,
        paging: false,
        searching: false,
        info: false,
        processing: true,
        dom: 't', 
        fixedColumns: true,
        scrollX: true,
        scrollCollapse: true,
        autoWidth: true,
        order: [],
        columnControl: ['orderStatus'],
        columnDefs: [
            {
            targets: 0,
            orderSequence: ['asc', 'desc', '']
            },
            {
            targets: '_all',
            orderable: true,
            orderSequence: ['desc', 'asc', '']
            }
        ],
        createdRow: function(row, data, dataIndex) {
            // Apply different classes based on player role
            if (mainPlayers.some(p => p.player_id === data.player_id)) {
                $(row).addClass('main-player-row');
                $('td', row).addClass('main-player-cell');
            } else {
                // Remaining rows
                $(row).addClass('sub-player-row');
                $('td', row).addClass('sub-player-cell');
            }
        }
    });

    // Set up eventlistener for window resize
    window.addEventListener('resize', () => {
        if ($.fn.dataTable) {
            $.fn.dataTable.tables({ visible: true, api: true }).columns.adjust();
        }
    });
}