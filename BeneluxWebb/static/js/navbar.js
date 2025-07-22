document.addEventListener('DOMContentLoaded', function () {
  const navList = document.getElementById('navList');
  const moreDropdown = document.getElementById('moreDropdown');
  const moreMenuList = document.getElementById('moreMenuList');

  function updateNavLayout() {
    // 1. Move all items back to the main navList (reset)
    const dropdownItems = Array.from(moreMenuList.children);
    dropdownItems.forEach(item => navList.insertBefore(item, moreDropdown));
    moreDropdown.classList.add('d-none');

    // 2. Get a live list of all nav items excluding "moreDropdown"
    const allItems = Array.from(navList.children).filter(item => item !== moreDropdown);

    // 3. Clear dropdown menu
    moreMenuList.innerHTML = '';

    // 4. Use requestAnimationFrame to ensure layout is calculated after DOM changes
    requestAnimationFrame(() => {
      const navListWidth = navList.offsetWidth;
      let totalWidth = moreDropdown.offsetWidth; // Start with dropdown width for reserve space

      for (let i = 0; i < allItems.length; i++) {
        const item = allItems[i];
        totalWidth += item.offsetWidth;

        if (totalWidth > navListWidth) {
          // Move overflowing items to dropdown menu
          for (let j = allItems.length - 1; j >= i; j--) {
            moreMenuList.insertBefore(allItems[j], moreMenuList.firstChild);
          }
          moreDropdown.classList.remove('d-none');
          break;
        }
      }
    });
  }

  window.addEventListener('resize', updateNavLayout);
  updateNavLayout();
});