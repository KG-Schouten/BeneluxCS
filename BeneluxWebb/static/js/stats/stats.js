document.addEventListener("DOMContentLoaded", () => {
    const sidebar = document.getElementById("sidebar-filters");
    const collapser = document.querySelector(".sidebar-collapser");

    // Create backdrop
    const backdrop = document.createElement("div");
    backdrop.classList.add("sidebar-backdrop");
    document.body.appendChild(backdrop);

    function openSidebar(smallScreen = false) {
        sidebar.classList.add("active");
        if (smallScreen) {
            backdrop.classList.add("active");
        }
    }

    function closeSidebar() {
        sidebar.classList.remove("active");
        backdrop.classList.remove("active");
    }

    function updateSidebarState(init = false) {
        if (window.innerWidth <= 768) {
            if (init) {
                closeSidebar();
            } else {
            }
        } else {
            openSidebar();
        }
    }

    // Initial run (force collapsed on mobile, open on desktop)
    updateSidebarState(true);

    // Only adjust on resize without auto-closing mobile sidebar
    window.addEventListener("resize", () => {
        updateSidebarState(false);
    });

    // Collapser toggle
    collapser.addEventListener("click", (e) => {
        e.stopPropagation();
        if (sidebar.classList.contains("active")) {
            closeSidebar();
        } else {
            openSidebar();
        }
    });

    // Only backdrop closes the sidebar
    backdrop.addEventListener("mousedown", (e) => {
        if (e.target === backdrop) {  // ensure it's not inside sidebar
            closeSidebar();
        }
    });

    backdrop.addEventListener("touchstart", (e) => {
        if (e.target === backdrop) {  // ensure it's not inside sidebar
            closeSidebar();
        }
    });
});
