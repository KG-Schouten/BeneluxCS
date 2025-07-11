document.addEventListener('DOMContentLoaded', function () {
  // Tooltip initialization function
  function initTooltips(context = document) {
    const tooltipTriggerList = [].slice.call(context.querySelectorAll('[data-bs-toggle="tooltip"]'));
    return tooltipTriggerList.map(function (el) {
      return new bootstrap.Tooltip(el);
    });
  }

  // Initialize tooltips on page load
  initTooltips();

  // ESEA tabs logic
  const tabs = document.querySelectorAll('#eseaTabs .nav-link, #eseaTabs button.nav-link');
  const content = document.getElementById('season-content');

  if (!tabs.length || !content) {
    // If no tabs or content found, abort script to prevent errors on other pages
    return;
  }

  // Load the last selected season from localStorage
  const lastSelectedSeason = localStorage.getItem('selectedSeason');

  tabs.forEach(tab => {
    tab.addEventListener('click', function () {
      const season = this.dataset.season;

      // Save the selected season to localStorage
      localStorage.setItem('selectedSeason', season);

      tabs.forEach(t => t.classList.remove('active'));
      this.classList.add('active');

      content.innerHTML = '<div class="text-muted">Loading...</div>';

      fetch(`/esea/season/${season}`)
        .then(res => {
          if (!res.ok) throw new Error("Failed to fetch");
          return res.text();
        })
        .then(html => {
          content.innerHTML = html;
          
          // Re-initialize tooltips
          initTooltips(content);
          
          // Initialize Bootstrap collapse elements
          initBootstrapCollapse();
          
          // Set up expand/collapse all button
          setupExpandAllButton();
        })
        .catch(err => {
          content.innerHTML = `<div class="text-danger">Error loading season: ${err}</div>`;
        });
    });
  });

  // Auto-click the last selected tab or the first tab if no selection is saved
  const defaultTab = lastSelectedSeason
    ? Array.from(tabs).find(tab => tab.dataset.season === lastSelectedSeason)
    : tabs[0];

  defaultTab?.click();
});

// Initialize Bootstrap collapse functionality
function initBootstrapCollapse() {
  // Use more specific selector to target only team collapse elements
  document.querySelectorAll('.team-card .collapse').forEach(collapse => {
    // Remove any existing event listeners
    collapse.removeEventListener('show.bs.collapse', updateIconOnShow);
    collapse.removeEventListener('hide.bs.collapse', updateIconOnHide);
    
    // Add event listeners for Bootstrap collapse events
    collapse.addEventListener('show.bs.collapse', updateIconOnShow);
    collapse.addEventListener('hide.bs.collapse', updateIconOnHide);
  });
}

// Update icon when collapse is shown
function updateIconOnShow(event) {
  const collapseId = event.target.id;
  const button = document.querySelector(`[data-bs-target="#${collapseId}"]`);
  if (button) {
    button.setAttribute('aria-expanded', 'true');
    const icon = button.querySelector('i.bi');
    if (icon) {
      icon.classList.remove('bi-chevron-down');
      icon.classList.add('bi-chevron-up');
    }
  }
}

// Update icon when collapse is hidden
function updateIconOnHide(event) {
  const collapseId = event.target.id;
  const button = document.querySelector(`[data-bs-target="#${collapseId}"]`);
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
    // Initialize button text based on current state
    updateExpandAllButtonText();
    
    expandAllBtn.removeEventListener('click', handleExpandAllClick);
    expandAllBtn.addEventListener('click', handleExpandAllClick);
  }
}

// Update expand all button text based on current collapse states
function updateExpandAllButtonText() {
  const expandAllBtn = document.getElementById('expandAllBtn');
  if (!expandAllBtn) return;
  
  // Use more specific selector for team collapse elements
  const collapses = document.querySelectorAll('.team-card .collapse');
  const allExpanded = Array.from(collapses).every(collapse => collapse.classList.contains('show'));
  expandAllBtn.textContent = allExpanded ? 'Collapse All' : 'Expand All';
}

// Handle expand/collapse all button clicks
function handleExpandAllClick() {
  // Use more specific selector for team collapse elements
  const collapses = document.querySelectorAll('.team-card .collapse');
  
  // Check if any collapse panels are currently hidden
  const anyCollapsed = Array.from(collapses).some(collapse => !collapse.classList.contains('show'));
  
  // Use Bootstrap's collapse methods for animation
  collapses.forEach(collapse => {
    // Get the Bootstrap collapse instance
    const bsCollapse = bootstrap.Collapse.getInstance(collapse) || new bootstrap.Collapse(collapse, {toggle: false});
    
    if (anyCollapsed) {
      // Show all
      bsCollapse.show();
    } else {
      // Hide all
      bsCollapse.hide();
    }
  });
  
  // Toggle button text accordingly
  this.textContent = anyCollapsed ? 'Collapse All' : 'Expand All';
}