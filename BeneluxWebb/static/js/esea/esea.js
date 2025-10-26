import { setupEseaTabs } from "./esea_season_tabs.js";
import { initDataTables } from "./esea_dataTables.js";

document.addEventListener('DOMContentLoaded', async function () {
  // Fetch from api/esea
  async function gatherEseaData() {
    try {
      const response = await fetch('/api/esea');
      if (!response.ok) throw new Error('Failed to fetch ESEA data');
      return await response.json();
    } catch (err) {
      console.error('Error fetching ESEA data:', err);
      return null;
    }
  }

  const data = await gatherEseaData();

  // --- INITIALIZATION ---
  let currentFetchController = null;

  // Add listener for season tab clicks
  document.addEventListener('eseaTabClicked', function (e) {
    const season = e.detail.season;
    loadSeasonContent(season);
  });

  // Initialize components on page load
  setupEseaTabs();

  // Re-run datatable on resize
  window.addEventListener('resize', () => {
    if ($.fn.dataTable) {
      $.fn.dataTable.tables({ visible: true, api: true }).columns.adjust();
    }
  });

  // Function to load season content
  function loadSeasonContent(season) {
      // Abort any ongoing fetch request
      if (currentFetchController) {
          currentFetchController.abort();
      }

      currentFetchController = new AbortController();
      const { signal } = currentFetchController;
      const content = document.getElementById('season-content');

      content.innerHTML = '<div class="loader" style="width: 45px;"></div>';

      fetch(`/esea/season/${season}`, { signal })
          .then(res => {
              if (!res.ok) throw new Error("Failed to fetch season data");
                  return res.text();
          })
          .then(html => {
              content.innerHTML = html;

              // Re-initialize components for the new content
              initTooltips(content);
              initButtonListeners(content);
          })
          .catch(err => {
          if (err.name === 'AbortError') return;
              content.innerHTML = `<div class="text-danger">Error loading season: ${err.message}</div>`;
          });
  }

  // Function to load team details
  function loadTeamDetails(team_id, season_number, collapseBody) {
    const team_name = collapseBody.dataset.teamName || "";

    // Already loaded? just resolve immediately.
    if (collapseBody.innerHTML.trim() !== "") {
      return Promise.resolve();
    }

    // Show loading spinner
    const detailsBtn = collapseBody.parentElement.previousElementSibling.querySelector('.details-btn');
    detailsBtn.innerHTML = '<span class="loader" style=""></span>';

    return fetch(`/esea/season/${season_number}/team/${team_id}/stats?team_name=${encodeURIComponent(team_name)}`)
      .then(res => {
        if (!res.ok) throw new Error("Failed to fetch team details");
        return res.text();
      })
      .then(html => {
        collapseBody.innerHTML = html;

        // Initialize tooltips in the newly loaded content
        initTooltips(collapseBody);
        initDataTables(collapseBody, data.columns_mapping);
      })
      .catch(err => {
        collapseBody.innerHTML = `<div class="text-danger">Error loading team details: ${err.message}</div>`;
      });
  }
  
  // --- COMPONENT INITIALIZATION FUNCTIONS ---
  // Initialize team card tooltips
  function initTooltips(context = document) {
    const tooltipTriggerList = [].slice.call(context.querySelectorAll('.esea-wrapper [data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(el => new bootstrap.Tooltip(el));
  }

  // Initialize team card button listeners
  function initButtonListeners(context = document) {
    const teamCards = context.querySelectorAll(".team-card");

    teamCards.forEach(card => {
      const detailsBtn = card.querySelector('.details-btn');
      const collapseDiv = card.querySelector('.collapse-body');

      detailsBtn.addEventListener('click', async () => {
        const teamId = detailsBtn.getAttribute('data-team-id');
        const seasonNumber = detailsBtn.getAttribute('data-season-number');
        const isCollapsed = collapseDiv.classList.contains('show');

        if (!isCollapsed) {
          detailsBtn.innerHTML = '<span class="loader" style=""></span>';

          await loadTeamDetails(teamId, seasonNumber, collapseDiv);
          
          collapseDiv.classList.add('show'); // only now show with content

          detailsBtn.innerHTML = '<i class="bi bi-chevron-up"></i>';
        } else {
          detailsBtn.innerHTML = '<i class="bi bi-chevron-down"></i>';
          collapseDiv.classList.remove('show');
        }
      });
    });
  }

});