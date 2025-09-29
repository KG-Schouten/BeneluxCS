document.addEventListener("DOMContentLoaded", () => {
    // Mark current active link
    document.querySelectorAll(".nav-links-sidebar a").forEach(link => {
        if (link.getAttribute("href") === window.location.pathname) {
            link.classList.add("active");
        }
    });

    // Initialize sidebar state based on window width
    const sidebar = document.getElementById("sidebar-filters");
    const collapser = document.querySelector(".sidebar-collapser");

    function updateSidebarState() {
        if (window.innerWidth <= 768) {
            sidebar.classList.remove("active"); // start collapsed on mobile
        } else {
            sidebar.classList.add("active"); // always expanded on desktop
        }
    }

    updateSidebarState();
    window.addEventListener("resize", updateSidebarState);

    // Collapser toggle
    collapser.addEventListener("click", (e) => {
        e.stopPropagation(); // prevent triggering the document click
        sidebar.classList.toggle("active");
    });

    document.addEventListener("click", (e) => {
        if (window.innerWidth <= 768 && sidebar.classList.contains("active")) {
            if (!sidebar.contains(e.target) && !collapser.contains(e.target)) {
                sidebar.classList.remove("active");
            }
        }
    });
});