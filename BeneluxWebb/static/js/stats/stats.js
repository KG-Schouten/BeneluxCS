document.addEventListener("DOMContentLoaded", () => {
    const navLinks_sidebar = document.querySelectorAll(".nav-links-sidebar a");
    const currentPath = window.location.pathname;

    navLinks_sidebar.forEach(link => {
        const linkPath = link.getAttribute("href");
        
        if (linkPath === currentPath) {
            link.classList.add("active");
        }
    });
});