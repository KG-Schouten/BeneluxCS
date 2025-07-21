document.addEventListener('DOMContentLoaded', function () {
  const toggleBtn = document.getElementById('darkModeToggle');
  const body = document.body;

  if (!toggleBtn) {
    console.log('[DarkMode] Toggle button not found. Exiting script.');
    return;
  }

  // Read saved mode or default to dark
  const mode = localStorage.getItem('theme') || 'dark';
  console.log('[DarkMode] Saved theme from localStorage:', mode);

  // Apply theme + set toggle state
  if (mode === 'light') {
    body.classList.add('light-mode');
    body.classList.remove('dark-mode');
    toggleBtn.checked = false; // Checkbox unchecked means light mode
    console.log('[DarkMode] Applied light mode.');
  } else {
    body.classList.add('dark-mode');
    body.classList.remove('light-mode');
    toggleBtn.checked = true; // Checkbox checked means dark mode
    console.log('[DarkMode] Applied dark mode.');
  }

  // Listen for checkbox state change
  toggleBtn.addEventListener('change', function () {
    if (toggleBtn.checked) {
      // Switch to dark mode
      body.classList.remove('light-mode');
      body.classList.add('dark-mode');
      localStorage.setItem('theme', 'dark');
      console.log('[DarkMode] Switched to dark mode. Saved in localStorage.');
    } else {
      // Switch to light mode
      body.classList.remove('dark-mode');
      body.classList.add('light-mode');
      localStorage.setItem('theme', 'light');
      console.log('[DarkMode] Switched to light mode. Saved in localStorage.');
    }
  });
});