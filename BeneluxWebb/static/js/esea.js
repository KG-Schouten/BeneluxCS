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

  tabs.forEach(tab => {
    tab.addEventListener('click', function () {
      const season = this.dataset.season;

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
          initTooltips(content); // Re-init tooltips inside loaded content
        })
        .catch(err => {
          content.innerHTML = `<div class="text-danger">Error loading season: ${err}</div>`;
        });
    });
  });

  // Auto-click first tab to load initial content
  tabs[0]?.click();
});