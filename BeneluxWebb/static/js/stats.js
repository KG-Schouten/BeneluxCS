import { SmartTable } from "./smart-table.js";

let smartTableInstance = null;

export function syncScrollbars() {
  const scrollableTable = document.getElementById("scrollable-table");
  const scrollbarTop = document.getElementById("scrollbar-top");

  if (!scrollableTable || !scrollbarTop) return;

  // Remove previous .scrollbar-inner if exists to avoid duplicates on reload
  const existingInner = scrollbarTop.querySelector(".scrollbar-inner");
  if (existingInner) {
    existingInner.remove();
  }

  // Create inner div to match table width for scrollbar
  const table = scrollableTable.querySelector("table");
  if (table) {
    const inner = document.createElement("div");
    inner.classList.add("scrollbar-inner");
    inner.style.width = table.offsetWidth + "px";
    scrollbarTop.appendChild(inner);
  }

  // Remove old event listeners before adding new ones
  scrollbarTop.onscroll = () => {
    scrollableTable.scrollLeft = scrollbarTop.scrollLeft;
    // Update scroll position in SmartTable instance only if not restoring
    if (smartTableInstance && !smartTableInstance.isRestoringScroll) {
      smartTableInstance.filters.scrollPosition = scrollbarTop.scrollLeft;
    }
  };

  scrollableTable.onscroll = () => {
    scrollbarTop.scrollLeft = scrollableTable.scrollLeft;
    // Update scroll position in SmartTable instance only if not restoring
    if (smartTableInstance && !smartTableInstance.isRestoringScroll) {
      smartTableInstance.filters.scrollPosition = scrollableTable.scrollLeft;
    }
  };
}

document.addEventListener("DOMContentLoaded", () => {
  smartTableInstance = new SmartTable("#stats-table-wrapper", {
    stateKey: "statsTableState",
    dataUrl: "/stats",
  });

  // Run once on load and after fetch (you can hook in fetchData to call this)
  syncScrollbars();
  
  // Make sure the table is initialized
  console.log("SmartTable initialized", table);
});