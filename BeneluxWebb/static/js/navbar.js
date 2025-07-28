document.addEventListener("DOMContentLoaded", () => {
    const hamburger = document.querySelector(".hamburger");
    const navLinks = document.querySelector(".nav-links");
    const links = document.querySelectorAll(".nav-links li");

    hamburger.addEventListener("click", () => {
        navLinks.classList.toggle("open");
        links.forEach(link => {
        link.classList.toggle("fade");
        });
        hamburger.classList.toggle("toggle");
    });

    window.addEventListener("resize", () => {
        if (window.innerWidth > 800) {
            document.querySelector(".nav-links").classList.remove("open");
            document.querySelectorAll(".nav-links li").forEach(link => {
            link.classList.remove("fade");
            });
            document.querySelector(".hamburger").classList.remove("toggle");
        }
    });
});