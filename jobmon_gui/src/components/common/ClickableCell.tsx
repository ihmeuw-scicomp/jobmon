import { styled } from '@mui/material/styles';


const ClickableCell = styled("div")(({ theme }) => ({
    cursor: "pointer",
    padding: theme.spacing(1),
    "&:hover": {
        backgroundColor: theme.palette.action.hover,
    },
}));

export default ClickableCell;
