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
  const tabs = document.querySelectorAll('#eseaTabs .nav-link');
  const content = document.getElementById('season-content');

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

  // Auto-click first tab
  tabs[0]?.click();

  // Dark mode toggle
  const toggleBtn = document.getElementById('darkModeToggle');
  const body = document.body;

  // Read saved mode or default to dark mode (no class needed if you want)
  const mode = localStorage.getItem('theme') || 'dark';

   if (mode === 'light') {
    body.classList.add('light-mode');
    body.classList.remove('dark-mode');
    toggleBtn.textContent = 'Dark Mode';
  } else {
    body.classList.add('dark-mode');
    body.classList.remove('light-mode');
    toggleBtn.textContent = 'Light Mode';
  }

  toggleBtn.addEventListener('click', function () {
    if (body.classList.contains('dark-mode')) {
      body.classList.remove('dark-mode');
      body.classList.add('light-mode');
      toggleBtn.textContent = 'Dark Mode';
      localStorage.setItem('theme', 'light');
    } else {
      body.classList.remove('light-mode');
      body.classList.add('dark-mode');
      toggleBtn.textContent = 'Light Mode';
      localStorage.setItem('theme', 'dark');
    }
  });
});