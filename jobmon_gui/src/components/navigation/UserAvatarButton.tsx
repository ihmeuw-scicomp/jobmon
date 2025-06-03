import { Avatar, IconButton } from '@mui/material';

const UserAvatarButton = ({ userFullName, onClickHandler }) => {
    const stringAvatar = name => {
        return {
            children:
                name === ''
                    ? null
                    : `${name.split(' ')[0][0]}${name.split(' ')[1][0]}`,
        };
    };

    return (
        <>
            <IconButton
                id="user-avatar-btn"
                onClick={e => {
                    onClickHandler(e.currentTarget);
                }}
            >
                <Avatar {...stringAvatar(userFullName)} />
            </IconButton>
        </>
    );
};

UserAvatarButton.defaultProps = {
    userFullName: '',
    onClickHandler: () => {},
};
export default UserAvatarButton;
