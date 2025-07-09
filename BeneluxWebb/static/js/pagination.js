export function bindPaginationEvents(containerSelector = '.pagination-link', onPageChange) {
    const paginationLinks = document.querySelectorAll(containerSelector);
    paginationLinks.forEach(link => {
        link.addEventListener('click', e => {
            e.preventDefault();
            const pageValue = parseInt(link.dataset.page || '1', 10);
            if (typeof onPageChange === 'function') {
                onPageChange(pageValue);
            }
        });
    });
}