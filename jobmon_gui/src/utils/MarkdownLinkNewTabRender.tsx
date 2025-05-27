export const MarkdownLinkNewTabRenderer = props => {
    return (
        <a href={props.href} target="_blank">
            {props.children}
        </a>
    );
};

export default MarkdownLinkNewTabRenderer;
