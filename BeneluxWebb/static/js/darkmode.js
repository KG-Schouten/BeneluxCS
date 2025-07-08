document.addEventListener('DOMContentLoaded', function () {
  const toggleBtn = document.getElementById('darkModeToggle');
  const body = document.body;

  if (!toggleBtn) {
    console.log('[DarkMode] Toggle button not found. Exiting script.');
    return;  // No toggle button found, do nothing
  }
  console.log('[DarkMode] Toggle button found:', toggleBtn);

  // Read saved mode or default to dark mode
  const mode = localStorage.getItem('theme') || 'dark';
  console.log('[DarkMode] Saved theme from localStorage:', mode);

  if (mode === 'light') {
    body.classList.add('light-mode');
    body.classList.remove('dark-mode');
    toggleBtn.textContent = 'Dark Mode';
    console.log('[DarkMode] Applied light mode.');
  } else {
    body.classList.add('dark-mode');
    body.classList.remove('light-mode');
    toggleBtn.textContent = 'Light Mode';
    console.log('[DarkMode] Applied dark mode.');
  }

  toggleBtn.addEventListener('click', function () {
    if (body.classList.contains('dark-mode')) {
      // Switch to light mode
      body.classList.remove('dark-mode');
      body.classList.add('light-mode');
      toggleBtn.textContent = 'Dark Mode';
      localStorage.setItem('theme', 'light');
      console.log('[DarkMode] Switched to light mode. Saved in localStorage.');
    } else {
      // Switch to dark mode
      body.classList.remove('light-mode');
      body.classList.add('dark-mode');
      toggleBtn.textContent = 'Light Mode';
      localStorage.setItem('theme', 'dark');
      console.log('[DarkMode] Switched to dark mode. Saved in localStorage.');
    }
  });
});