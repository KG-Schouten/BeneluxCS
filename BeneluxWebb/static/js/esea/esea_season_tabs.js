export function setupEseaTabs() {
    const tabs = document.querySelectorAll('#eseaTabs .nav-item');
    const content = document.getElementById('season-content');

    const dropdownBtn = document.getElementById('DropdownBtn');
    const dropdownMenu = document.getElementById('DropdownMenu');

    if (!tabs.length || !content) {
      return; // Abort if critical elements are missing
    }

    const lastSelectedSeason = localStorage.getItem('selectedSeason');

    const tabClicked = new CustomEvent('eseaTabClicked', {
      detail: { season: null }
    });

    // Set up click listeners for each tab
    tabs.forEach(tab => {
      tab.addEventListener('click', function () {
        const season = this.dataset.season;
        localStorage.setItem('selectedSeason', season);

        // Update active states
        tabs.forEach(t => t.classList.remove('active'));
        this.classList.add('active');

        // Update event detail
        tabClicked.detail.season = season;
        document.dispatchEvent(tabClicked);
      });
    });

    // Set up click listeners for dropdown menu button
    dropdownBtn.addEventListener('click', function () {
        dropdownMenu.classList.toggle('show');
    });
    window.addEventListener('click', function (event) {
        if (!dropdownBtn.contains(event.target)) {
            dropdownMenu.classList.remove('show');
        }
    });

    // Auto-click the last selected tab or the first one
    const defaultTab = lastSelectedSeason
      ? Array.from(tabs).find(tab => tab.dataset.season === lastSelectedSeason)
      : tabs[0];

    defaultTab?.click();

    // Rerun autocollapse on window resize
    window.addEventListener('resize', () => {
        autocollapse();
    })
    
    // autocollapse on setup
    autocollapse();
}

function autocollapse() {
    const navBox = document.querySelector('.season-nav-box');
    const navTabs = document.querySelector('#eseaTabs');

    const dropdown = document.getElementById('eseaDropdown');
    const dropdownBtn = document.getElementById('DropdownBtn');
    const dropdownMenu = document.getElementById('DropdownMenu');

    const tabsWidth = navTabs.offsetWidth;
    const parentWidth = navBox.offsetWidth;
    const dropdownBtnWidth = dropdownBtn.offsetWidth;

    // Get tabs children
    const tabsChildren = Array.from(navTabs.children).map(child => ({
        el: child,
        width: child.offsetWidth
    }));
    const last_tab_width = tabsChildren[tabsChildren.length - 1]?.width || 0;

    // Get dropdown children
    const dropdownChildren = Array.from(dropdownMenu.children).map(child => ({
        el: child,
        width: last_tab_width
    }));
    
    let first_dropdown_item_width = 0;
    if (dropdownChildren.length > 0) {
        first_dropdown_item_width = dropdownChildren[0]?.width || 0;
    } else {
        first_dropdown_item_width = 0;
    }

    // Determine the width of the tabs (and dropdown if visible)
    const margin = 10;
    let availableWidth = parentWidth - margin;
    if (!dropdown.classList.contains('d-none')) {
        availableWidth -= dropdownBtnWidth;
    }

    if (availableWidth <= tabsWidth) {
        navBox.style.justifyContent = 'flex-start';
        dropdown.classList.remove('d-none');

        // Move last child in tabsChildren to dropdownMenu
        let last_tab = tabsChildren[tabsChildren.length - 1];
        dropdownMenu.insertBefore(navTabs.removeChild(last_tab.el), dropdownMenu.firstChild);

        // Re-evaluate to see if we need to move more items
        autocollapse();
        
    } else {
        navBox.style.justifyContent = 'center';
    }

    if (availableWidth >= (tabsWidth + first_dropdown_item_width) && dropdownChildren.length > 0) {
        // Move first child in dropdownMenu to tabs
        navTabs.appendChild(dropdownMenu.removeChild(dropdownChildren[0].el));

        if (dropdownMenu.children.length === 0) {
            dropdown.classList.add('d-none');
            navBox.style.justifyContent = 'center'; 
        }

        // Re-evaluate to see if we can move more items
        autocollapse();
    }
}