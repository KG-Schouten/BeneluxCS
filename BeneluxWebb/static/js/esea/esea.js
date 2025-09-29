document.addEventListener('DOMContentLoaded', async function () {

  // --- INITIALIZATION ---
  // Gather variables from flask api
  try {
    const response = await fetch('/api/esea');
    const meta = await response.json();
    var columns_mapping = meta.columns_mapping || {};
  } catch (error) {
    console.error("Error fetching ESEA metadata:", error);
    var columns_mapping = {};
  }

  // Initialize components on page load
  initTooltips();
  setupEseaTabs();
  autocollapse('#eseaTabs', 40);

  // Re-run autocollapse on resize
  window.addEventListener('resize', () => {
    autocollapse('#eseaTabs', 40);
    // Adjust all visible DataTables on resize so headers/body stay aligned
    if ($.fn.dataTable) {
      $.fn.dataTable.tables({ visible: true, api: true }).columns.adjust();
    }
  });

  // --- ESEA TABS LOGIC ---
  function setupEseaTabs() {
    const tabs = document.querySelectorAll('#eseaTabs .nav-link:not(.dropdown-toggle), #eseaTabs button.nav-link:not(.dropdown-toggle)');
    const content = document.getElementById('season-content');

    if (!tabs.length || !content) {
      return; // Abort if critical elements are missing
    }

    const lastSelectedSeason = localStorage.getItem('selectedSeason');

    tabs.forEach(tab => {
      tab.addEventListener('click', function () {
        const season = this.dataset.season;
        localStorage.setItem('selectedSeason', season);

        // Update active states
        tabs.forEach(t => t.classList.remove('active'));
        this.classList.add('active');

        loadSeasonContent(season, content);
      });
    });

    // Auto-click the last selected tab or the first one
    const defaultTab = lastSelectedSeason
      ? Array.from(tabs).find(tab => tab.dataset.season === lastSelectedSeason)
      : tabs[0];

    defaultTab?.click();
  }

  function loadSeasonContent(season, content) {
    content.innerHTML = '<div class="text-muted">Loading...</div>';

    fetch(`/esea/season/${season}`)
      .then(res => {
        if (!res.ok) throw new Error("Failed to fetch season data");
        return res.text();
      })
      .then(html => {
        content.innerHTML = html;

        // Re-initialize components for the new content
        initTooltips(content);
        initBootstrapCollapse();
        setupExpandAllButton();
        initDataTables(content); // Initialize DataTables for the new content
      })
      .catch(err => {
        content.innerHTML = `<div class="text-danger">Error loading season: ${err.message}</div>`;
      });
  }

  // --- COMPONENT INITIALIZATION FUNCTIONS ---

  // Initialize all tooltips
  function initTooltips(context = document) {
    const tooltipTriggerList = [].slice.call(context.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(el => new bootstrap.Tooltip(el));
  }

  // Initialize all DataTables
  function initDataTables(context = document) {
    context.querySelectorAll("input[id^='player_stats_data_']").forEach(input => {
      const teamId = input.id.replace('player_stats_data_', '');
      const tableId = `#player_stats_table_${teamId}`;

      const playerStats = JSON.parse(input.dataset.stats);

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
          if (dataIndex < 5) {
            // First 5 rows
            $(row).addClass('main-player-row'); // <-- your custom class
            $('td', row).addClass('main-player-cell');
          } else {
            // Remaining rows
            $(row).addClass('sub-player-row');
            $('td', row).addClass('sub-player-cell');
          }
        }
      });
    });
  }

  // Initialize Bootstrap collapse functionality
  function initBootstrapCollapse() {
    document.querySelectorAll('.team-card .collapse').forEach(collapse => {
      collapse.addEventListener('shown.bs.collapse', function () {
        const table = this.querySelector('table');
        if (table && $.fn.dataTable.isDataTable(table)) {
          $(table).DataTable().columns.adjust();
        }
      });
      collapse.removeEventListener('show.bs.collapse', updateIconOnShow);
      collapse.removeEventListener('hide.bs.collapse', updateIconOnHide);
      collapse.addEventListener('show.bs.collapse', updateIconOnShow);
      collapse.addEventListener('hide.bs.collapse', updateIconOnHide);
    });
  }

  // --- COLLAPSE/EXPAND LOGIC ---

  // Update icon when a collapse panel is shown
  function updateIconOnShow(event) {
    const button = document.querySelector(`[data-bs-target="#${event.target.id}"]`);
    if (button) {
      button.setAttribute('aria-expanded', 'true');
      const icon = button.querySelector('i.bi');
      if (icon) {
        icon.classList.remove('bi-chevron-down');
        icon.classList.add('bi-chevron-up');
      }
    }
  }

  // Update icon when a collapse panel is hidden
  function updateIconOnHide(event) {
    const button = document.querySelector(`[data-bs-target="#${event.target.id}"]`);
    if (button) {
      button.setAttribute('aria-expanded', 'false');
      const icon = button.querySelector('i.bi');
      if (icon) {
        icon.classList.remove('bi-chevron-up');
        icon.classList.add('bi-chevron-down');
      }
    }
  }

  // Setup expand/collapse all functionality
  function setupExpandAllButton() {
    const expandAllBtn = document.getElementById('expandAllBtn');
    if (expandAllBtn) {
      updateExpandAllButtonText();
      expandAllBtn.removeEventListener('click', handleExpandAllClick);
      expandAllBtn.addEventListener('click', handleExpandAllClick);
    }
  }

  // Update the text of the "Expand All" button
  function updateExpandAllButtonText() {
    const expandAllBtn = document.getElementById('expandAllBtn');
    if (!expandAllBtn) return;

    const collapses = document.querySelectorAll('.team-card .collapse');
    const allExpanded = Array.from(collapses).every(c => c.classList.contains('show'));
    expandAllBtn.textContent = allExpanded ? 'Collapse All' : 'Expand All';
  }

  // Handle clicks on the "Expand All" button
  function handleExpandAllClick() {
    const collapses = document.querySelectorAll('.team-card .collapse');
    const anyCollapsed = Array.from(collapses).some(c => !c.classList.contains('show'));

    collapses.forEach(collapse => {
      const bsCollapse = bootstrap.Collapse.getInstance(collapse) || new bootstrap.Collapse(collapse, { toggle: false });
      anyCollapsed ? bsCollapse.show() : bsCollapse.hide();
    });

    this.textContent = anyCollapsed ? 'Collapse All' : 'Expand All';
  }

  // --- AUTO-COLLAPSE TABS ---

  // Auto-collapse overflowing tabs into a dropdown
  function autocollapse(menuSelector, maxHeight) {
    const nav = document.querySelector(menuSelector);
    if (!nav) return;
    
    const dropdown = nav.querySelector('.dropdown');
    const dropdownMenu = dropdown.querySelector('.dropdown-menu');

    function getNavHeight() {
      return nav.getBoundingClientRect().height;
    }

    function moveLastVisibleToDropdown() {
      const items = Array.from(nav.children).filter(li => !li.classList.contains('dropdown'));
      if (items.length > 0) {
        dropdownMenu.insertBefore(items[items.length - 1], dropdownMenu.firstChild);
      }
    }

    function moveFirstDropdownItemBack() {
      const dropdownItems = Array.from(dropdownMenu.children);
      if (dropdownItems.length > 0) {
        nav.insertBefore(dropdownItems[0], dropdown);
      }
    }

    if (getNavHeight() >= maxHeight) {
      dropdown.classList.remove('d-none');
      nav.classList.remove('w-auto');
      nav.classList.add('w-100');

      while (getNavHeight() > maxHeight) {
        moveLastVisibleToDropdown();
      }

      nav.classList.add('w-auto');
      nav.classList.remove('w-100');
    } else {
      let collapsedItems = Array.from(dropdownMenu.children);
      
      if (collapsedItems.length === 0) {
        dropdown.classList.add('d-none');
      }

      while (getNavHeight() < maxHeight && collapsedItems.length > 0) {
        moveFirstDropdownItemBack();
        collapsedItems = Array.from(dropdownMenu.children);
      }

      if (getNavHeight() > maxHeight) {
        autocollapse(menuSelector, maxHeight); // Re-check in case we overflow again
      }
    }
  }
});