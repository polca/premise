function openExternalLinksInNewTabs() {
  for (const link of document.querySelectorAll("a[href]")) {
    if (
      (link.protocol === "https:" || link.protocol === "http:") &&
      link.origin !== window.location.origin
    ) {
      link.target = "_blank";
      link.relList.add("noopener", "noreferrer");
      link.title = link.title
        ? `${link.title} (opens in a new tab)`
        : "Opens in a new tab";
    }
  }
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", openExternalLinksInNewTabs, {
    once: true,
  });
} else {
  openExternalLinksInNewTabs();
}
