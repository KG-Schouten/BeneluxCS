document.addEventListener("DOMContentLoaded", () => {
    const hamburger = document.querySelector(".hamburger");
    const navContainer = document.querySelector(".navbar-container");
    const navList = document.querySelector(".nav-links");
    const navLinkItems = document.querySelectorAll(".nav-links li");
    const navLinks = document.querySelectorAll(".nav-links li a");
    const navbar = document.querySelector(".navbar-container");
    const currentPath = window.location.pathname;

    // Set active link based on current path
    navLinks.forEach(link => {
        const linkPath = link.getAttribute("href");
        if (currentPath.includes(linkPath) && linkPath !== "/") {
        link.classList.add("active");
        }
    });

    // Mobile menu toggle
    hamburger.addEventListener("click", () => {
        navContainer.classList.toggle("open");
        navLinkItems.forEach(link => {
        link.classList.toggle("fade");
        });
        hamburger.classList.toggle("toggle");
    });

    // Reset menu when window is resized
    window.addEventListener("resize", () => {
        if (window.innerWidth > 800) {
        navList.classList.remove("open");
        navLinkItems.forEach(link => link.classList.remove("fade"));
        hamburger.classList.remove("toggle");
        }
    });

    // Navbar scroll behavior
    window.addEventListener("scroll", () => {
        if (window.scrollY > 50) {
        navbar.classList.add("scrolled");
        } else {
        navbar.classList.remove("scrolled");
        }
    });

    // Initialize tooltips for disabled links
    const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]')
    tooltipTriggerList.forEach(el => {
        new bootstrap.Tooltip(el)
    })
});