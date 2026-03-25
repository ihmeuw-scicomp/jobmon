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
                size="small"
                sx={{ p: 0.5 }}
                onClick={e => {
                    onClickHandler(e.currentTarget);
                }}
            >
                <Avatar
                    {...stringAvatar(userFullName)}
                    sx={{ width: 24, height: 24, fontSize: '0.75rem' }}
                />
            </IconButton>
        </>
    );
};

UserAvatarButton.defaultProps = {
    userFullName: '',
    onClickHandler: () => {},
};
export default UserAvatarButton;
